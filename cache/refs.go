package cache

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/snapshot"
	"github.com/moby/buildkit/util/compression"
	"github.com/moby/buildkit/util/flightcontrol"
	"github.com/moby/buildkit/util/leaseutil"
	"github.com/moby/buildkit/util/winlayers"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// Ref is a reference to cacheable objects.
type Ref interface {
	Mountable
	Metadata
	Release(context.Context) error
	ToImmutable(context.Context) (*ImmutableRef, error)
}

type Mountable interface {
	Mount(ctx context.Context, readonly bool, s session.Group) (snapshot.Mountable, error)
}

type ImmutableRef struct {
	*cacheRecord
	triggerLastUsed bool
	descHandlers    DescHandlers
}

type MutableRef struct {
	*cacheRecord
	triggerLastUsed bool
	descHandlers    DescHandlers
}

type cacheRecord struct {
	cm     *cacheManager
	mu     sync.Mutex
	sizeG  flightcontrol.Group
	parent *ImmutableRef
	*cacheMetadata

	// immutableRefs keeps track of each ref pointing to this cacheRecord that can't change the underlying snapshot
	// data. When it's empty and mutableRef below is empty, that means there's no more unreleased pointers to this
	// struct and the cacheRecord can be considered for deletion. We enforce that there can not be immutableRefs while
	// mutableRef is set.
	immutableRefs map[*ImmutableRef]struct{}

	// mutableRef keeps track of a ref to this cacheRecord whose snapshot can be mounted read-write.
	// We enforce that at most one mutable ref points to this cacheRecord at a time and no immutableRefs
	// are set while mutableRef is set.
	mutableRef *MutableRef

	// isCommitted means the underlying snapshot has been committed to its driver
	isCommitted bool

	// dead means record is marked as deleted
	dead bool

	view      string
	viewMount snapshot.Mountable

	parentChainCache []digest.Digest
}

// hold cacheRecord.mu before calling
func (cr *cacheRecord) ref(triggerLastUsed bool, descHandlers DescHandlers) (*ImmutableRef, error) {
	if cr.mutableRef != nil {
		return nil, ErrLocked
	}

	r := &ImmutableRef{
		cacheRecord:     cr,
		triggerLastUsed: triggerLastUsed,
		descHandlers:    descHandlers,
	}
	cr.immutableRefs[r] = struct{}{}
	return r, nil
}

// hold cacheRecord.mu lock before calling
func (cr *cacheRecord) mref(triggerLastUsed bool, descHandlers DescHandlers) (*MutableRef, error) {
	if cr.isCommitted {
		return nil, errors.Wrap(errInvalid, "cannot get mutable ref of committed cache record")
	}
	if cr.mutableRef != nil {
		return nil, ErrLocked
	}
	if len(cr.immutableRefs) > 0 {
		return nil, ErrLocked
	}

	r := &MutableRef{
		cacheRecord:     cr,
		triggerLastUsed: triggerLastUsed,
		descHandlers:    descHandlers,
	}
	cr.mutableRef = r
	return r, nil
}

// hold cacheRecord.mu lock before calling
func (cr *cacheRecord) refCount() int {
	count := len(cr.immutableRefs)
	if cr.mutableRef != nil {
		count++
	}
	return count
}

func (cr *cacheRecord) parentChain() []digest.Digest {
	if cr.parentChainCache != nil {
		return cr.parentChainCache
	}
	blob := cr.getBlob()
	if blob == "" {
		return nil
	}

	var parent []digest.Digest
	if cr.parent != nil {
		parent = cr.parent.parentChain()
	}
	pcc := make([]digest.Digest, len(parent)+1)
	copy(pcc, parent)
	pcc[len(parent)] = digest.Digest(blob)
	cr.parentChainCache = pcc
	return pcc
}

func (cr *cacheRecord) isLazy(ctx context.Context) (bool, error) {
	if !cr.getBlobOnly() {
		return false, nil
	}
	dgst := cr.getBlob()
	// special case for moby where there is no compressed blob (empty digest)
	if dgst == "" {
		return false, nil
	}
	_, err := cr.cm.ContentStore.Info(ctx, dgst)
	if errors.Is(err, errdefs.ErrNotFound) {
		return true, nil
	} else if err != nil {
		return false, err
	}

	// If the snapshot is a remote snapshot, this layer is lazy.
	if info, err := cr.cm.Snapshotter.Stat(ctx, cr.getSnapshotID()); err == nil {
		if _, ok := info.Labels["containerd.io/snapshot/remote"]; ok {
			return true, nil
		}
	}

	return false, nil
}

func (cr *cacheRecord) size(ctx context.Context) (int64, error) {
	// this expects that usage() is implemented lazily
	s, err := cr.sizeG.Do(ctx, cr.ID(), func(ctx context.Context) (interface{}, error) {
		cr.mu.Lock()
		s := cr.getSize()
		if s != sizeUnknown {
			cr.mu.Unlock()
			return s, nil
		}
		driverID := cr.getSnapshotID()
		cr.mu.Unlock()
		var usage snapshots.Usage
		if !cr.getBlobOnly() {
			var err error
			usage, err = cr.cm.ManagerOpt.Snapshotter.Usage(ctx, driverID)
			if err != nil {
				cr.mu.Lock()
				isDead := cr.dead
				cr.mu.Unlock()
				if isDead {
					return int64(0), nil
				}
				if !errors.Is(err, errdefs.ErrNotFound) {
					return s, errors.Wrapf(err, "failed to get usage for %s", cr.ID())
				}
			}
		}
		if dgst := cr.getBlob(); dgst != "" {
			info, err := cr.cm.ContentStore.Info(ctx, digest.Digest(dgst))
			if err == nil {
				usage.Size += info.Size
			}
			for k, v := range info.Labels {
				// accumulate size of compression variant blobs
				if strings.HasPrefix(k, compressionVariantDigestLabelPrefix) {
					if cdgst, err := digest.Parse(v); err == nil {
						if digest.Digest(dgst) == cdgst {
							// do not double count if the label points to this content itself.
							continue
						}
						if info, err := cr.cm.ContentStore.Info(ctx, cdgst); err == nil {
							usage.Size += info.Size
						}
					}
				}
			}
		}
		cr.mu.Lock()
		cr.queueSize(usage.Size)
		if err := cr.CommitMetadata(); err != nil {
			cr.mu.Unlock()
			return s, err
		}
		cr.mu.Unlock()
		return usage.Size, nil
	})
	if err != nil {
		return 0, err
	}
	return s.(int64), nil
}

// call when holding the manager lock
func (cr *cacheRecord) remove(ctx context.Context) error {
	delete(cr.cm.records, cr.ID())
	if cr.parent != nil {
		cr.parent.mu.Lock()
		err := cr.parent.release(ctx)
		cr.parent.mu.Unlock()
		if err != nil {
			return err
		}
	}
	if err := cr.cm.LeaseManager.Delete(context.TODO(), leases.Lease{ID: cr.ID()}); err != nil {
		return errors.Wrapf(err, "failed to remove %s", cr.ID())
	}
	if err := cr.cm.md.Clear(cr.ID()); err != nil {
		return err
	}
	return nil
}

// hold cacheRecord.mu lock before calling
func (cr *cacheRecord) release(ctx context.Context, forceKeep bool) error {
	if cr.refCount() > 0 {
		return nil
	}

	if cr.viewMount != nil { // TODO: release viewMount earlier if possible
		if err := cr.cm.LeaseManager.Delete(ctx, leases.Lease{ID: cr.view}); err != nil {
			return errors.Wrapf(err, "failed to remove view lease %s", cr.view)
		}
		cr.view = ""
		cr.viewMount = nil
	}

	if cr.HasCachePolicyRetain() {
		return nil
	}

	if forceKeep {
		return nil
	}
	return cr.remove(ctx)
}

func (sr *ImmutableRef) Clone() *ImmutableRef {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.clone(false)
}

// hold cacheRecord.mu lock before calling
func (sr *ImmutableRef) clone(triggerLastUsed bool) *ImmutableRef {
	ir2 := &ImmutableRef{
		cacheRecord:     sr.cacheRecord,
		triggerLastUsed: triggerLastUsed,
		descHandlers:    sr.descHandlers,
	}
	ir2.immutableRefs[ir2] = struct{}{}
	return ir2
}

func (sr *ImmutableRef) Parent() *ImmutableRef {
	p := sr.parent
	if p == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	return p.clone(true)
}

func (sr *ImmutableRef) ociDesc() (ocispec.Descriptor, error) {
	desc := ocispec.Descriptor{
		Digest:      sr.getBlob(),
		Size:        sr.getBlobSize(),
		MediaType:   sr.getMediaType(),
		Annotations: make(map[string]string),
	}

	diffID := sr.getDiffID()
	if diffID != "" {
		desc.Annotations["containerd.io/uncompressed"] = string(diffID)
	}

	createdAt := sr.GetCreatedAt()
	if !createdAt.IsZero() {
		createdAt, err := createdAt.MarshalText()
		if err != nil {
			return ocispec.Descriptor{}, err
		}
		desc.Annotations["buildkit/createdat"] = string(createdAt)
	}

	return desc, nil
}

const compressionVariantDigestLabelPrefix = "buildkit.io/compression/digest."

func compressionVariantDigestLabel(compressionType compression.Type) string {
	return compressionVariantDigestLabelPrefix + compressionType.String()
}

func (sr *ImmutableRef) getCompressionBlob(ctx context.Context, compressionType compression.Type) (content.Info, error) {
	cs := sr.cm.ContentStore
	info, err := cs.Info(ctx, sr.getBlob())
	if err != nil {
		return content.Info{}, err
	}
	dgstS, ok := info.Labels[compressionVariantDigestLabel(compressionType)]
	if ok {
		dgst, err := digest.Parse(dgstS)
		if err != nil {
			return content.Info{}, err
		}
		return cs.Info(ctx, dgst)
	}
	return content.Info{}, errdefs.ErrNotFound
}

func (sr *ImmutableRef) addCompressionBlob(ctx context.Context, dgst digest.Digest, compressionType compression.Type) error {
	cs := sr.cm.ContentStore
	if err := sr.cm.ManagerOpt.LeaseManager.AddResource(ctx, leases.Lease{ID: sr.ID()}, leases.Resource{
		ID:   dgst.String(),
		Type: "content",
	}); err != nil {
		return err
	}
	info, err := cs.Info(ctx, sr.getBlob())
	if err != nil {
		return err
	}
	if info.Labels == nil {
		info.Labels = make(map[string]string)
	}
	cachedVariantLabel := compressionVariantDigestLabel(compressionType)
	info.Labels[cachedVariantLabel] = dgst.String()
	if _, err := cs.Update(ctx, info, "labels."+cachedVariantLabel); err != nil {
		return err
	}
	return nil
}

// order is from parent->child, sr will be at end of slice
func (sr *ImmutableRef) parentRefChain() []*ImmutableRef {
	var count int
	for ref := sr; ref != nil; ref = ref.parent {
		count++
	}
	refs := make([]*ImmutableRef, count)
	for i, ref := count-1, sr; ref != nil; i, ref = i-1, ref.parent {
		refs[i] = ref
	}
	return refs
}

func (sr *ImmutableRef) Mount(ctx context.Context, readonly bool, s session.Group) (snapshot.Mountable, error) {
	if err := sr.Extract(ctx, s); err != nil {
		return nil, err
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.cm.Snapshotter.Name() == "stargz" {
		var (
			m    snapshot.Mountable
			rerr error
		)
		if err := sr.withRemoteSnapshotLabelsStargzMode(ctx, s, func() {
			m, rerr = sr.mount(ctx)
		}); err != nil {
			return nil, err
		}
		return m, rerr
	}

	// all mounts for immutable refs are read-only, so the readonly param is ignored here
	return sr.mount(ctx)
}

// must be called holding cacheRecord mu
func (sr *ImmutableRef) mount(ctx context.Context) (snapshot.Mountable, error) {
	if !sr.isCommitted {
		// if not committed, there must be a mutable ref still around, return its mounts
		// but set read-only
		m, err := sr.cm.Snapshotter.Mounts(ctx, sr.getSnapshotID())
		if err != nil {
			return nil, errors.Wrapf(err, "failed to mount %s", sr.ID())
		}
		m = setReadonly(m)
		return m, nil
	}

	if sr.viewMount == nil {
		view := identity.NewID()
		l, err := sr.cm.LeaseManager.Create(ctx, func(l *leases.Lease) error {
			l.ID = view
			l.Labels = map[string]string{
				"containerd.io/gc.flat": time.Now().UTC().Format(time.RFC3339Nano),
			}
			return nil
		}, leaseutil.MakeTemporary)
		if err != nil {
			return nil, err
		}
		ctx = leases.WithLease(ctx, l.ID)
		m, err := sr.cm.Snapshotter.View(ctx, view, sr.getSnapshotID())
		if err != nil {
			sr.cm.LeaseManager.Delete(context.TODO(), leases.Lease{ID: l.ID})
			return nil, errors.Wrapf(err, "failed to mount %s", sr.ID())
		}
		sr.view = view
		sr.viewMount = m
	}
	return sr.viewMount, nil
}

func (sr *ImmutableRef) Extract(ctx context.Context, s session.Group) (rerr error) {
	if !sr.getBlobOnly() {
		return
	}

	ctx, done, err := leaseutil.WithLease(ctx, sr.cm.LeaseManager, leaseutil.MakeTemporary)
	if err != nil {
		return err
	}
	defer done(ctx)

	if sr.GetLayerType() == "windows" {
		ctx = winlayers.UseWindowsLayerMode(ctx)
	}

	if sr.cm.Snapshotter.Name() == "stargz" {
		if err := sr.withRemoteSnapshotLabelsStargzMode(ctx, s, func() {
			if rerr = sr.prepareRemoteSnapshotsStargzMode(ctx, s); rerr != nil {
				return
			}
			rerr = sr.extract(ctx, sr.descHandlers, s)
		}); err != nil {
			return err
		}
		return rerr
	}

	return sr.extract(ctx, sr.descHandlers, s)
}

func (sr *ImmutableRef) withRemoteSnapshotLabelsStargzMode(ctx context.Context, s session.Group, f func()) error {
	dhs := sr.descHandlers
	for _, r := range sr.parentRefChain() {
		r := r
		info, err := r.cm.Snapshotter.Stat(ctx, r.getSnapshotID())
		if err != nil && !errdefs.IsNotFound(err) {
			return err
		} else if errdefs.IsNotFound(err) {
			continue // This snpashot doesn't exist; skip
		} else if _, ok := info.Labels["containerd.io/snapshot/remote"]; !ok {
			continue // This isn't a remote snapshot; skip
		}
		desc, err := r.ociDesc()
		if err != nil {
			return err
		}
		dh := dhs[desc.Digest]
		if dh == nil {
			continue // no info passed; skip
		}

		// Append temporary labels (based on dh.SnapshotLabels) as hints for remote snapshots.
		// For avoiding collosion among calls, keys of these tmp labels contain an unique ID.
		flds, labels := makeTmpLabelsStargzMode(snapshots.FilterInheritedLabels(dh.SnapshotLabels), s)
		info.Labels = labels
		if _, err := r.cm.Snapshotter.Update(ctx, info, flds...); err != nil {
			return errors.Wrapf(err, "failed to add tmp remote labels for remote snapshot")
		}
		defer func() {
			for k := range info.Labels {
				info.Labels[k] = "" // Remove labels appended in this call
			}
			if _, err := r.cm.Snapshotter.Update(ctx, info, flds...); err != nil {
				logrus.Warn(errors.Wrapf(err, "failed to remove tmp remote labels"))
			}
		}()

		continue
	}

	f()

	return nil
}

func (sr *ImmutableRef) prepareRemoteSnapshotsStargzMode(ctx context.Context, s session.Group) error {
	_, err := sr.sizeG.Do(ctx, sr.ID()+"-prepare-remote-snapshot", func(ctx context.Context) (_ interface{}, rerr error) {
		dhs := sr.descHandlers
		for _, r := range sr.parentRefChain() {
			r := r
			snapshotID := r.getSnapshotID()
			if _, err := r.cm.Snapshotter.Stat(ctx, snapshotID); err == nil {
				continue
			}

			desc, err := r.ociDesc()
			if err != nil {
				return nil, err
			}
			dh := dhs[desc.Digest]
			if dh == nil {
				// We cannot prepare remote snapshots without descHandler.
				return nil, nil
			}

			// tmpLabels contains dh.SnapshotLabels + session IDs. All keys contain
			// an unique ID for avoiding the collision among snapshotter API calls to
			// this snapshot. tmpLabels will be removed at the end of this function.
			defaultLabels := snapshots.FilterInheritedLabels(dh.SnapshotLabels)
			if defaultLabels == nil {
				defaultLabels = make(map[string]string)
			}
			tmpFields, tmpLabels := makeTmpLabelsStargzMode(defaultLabels, s)
			defaultLabels["containerd.io/snapshot.ref"] = snapshotID

			// Prepare remote snapshots
			var (
				key  = fmt.Sprintf("tmp-%s %s", identity.NewID(), r.getChainID())
				opts = []snapshots.Opt{
					snapshots.WithLabels(defaultLabels),
					snapshots.WithLabels(tmpLabels),
				}
			)
			parentID := ""
			if r.parent != nil {
				parentID = r.parent.getSnapshotID()
			}
			if err = r.cm.Snapshotter.Prepare(ctx, key, parentID, opts...); err != nil {
				if errdefs.IsAlreadyExists(err) {
					// Check if the targeting snapshot ID has been prepared as
					// a remote snapshot in the snapshotter.
					info, err := r.cm.Snapshotter.Stat(ctx, snapshotID)
					if err == nil { // usable as remote snapshot without unlazying.
						defer func() {
							// Remove tmp labels appended in this func
							for k := range tmpLabels {
								info.Labels[k] = ""
							}
							if _, err := r.cm.Snapshotter.Update(ctx, info, tmpFields...); err != nil {
								logrus.Warn(errors.Wrapf(err,
									"failed to remove tmp remote labels after prepare"))
							}
						}()

						// Try the next layer as well.
						continue
					}
				}
			}

			// This layer and all upper layers cannot be prepared without unlazying.
			break
		}

		return nil, nil
	})
	return err
}

func makeTmpLabelsStargzMode(labels map[string]string, s session.Group) (fields []string, res map[string]string) {
	res = make(map[string]string)
	// Append unique ID to labels for avoiding collision of labels among calls
	id := identity.NewID()
	for k, v := range labels {
		tmpKey := k + "." + id
		fields = append(fields, "labels."+tmpKey)
		res[tmpKey] = v
	}
	for i, sid := range session.AllSessionIDs(s) {
		sidKey := "containerd.io/snapshot/remote/stargz.session." + fmt.Sprintf("%d", i) + "." + id
		fields = append(fields, "labels."+sidKey)
		res[sidKey] = sid
	}
	return
}

func (sr *ImmutableRef) extract(ctx context.Context, dhs DescHandlers, s session.Group) error {
	_, err := sr.sizeG.Do(ctx, sr.ID()+"-extract", func(ctx context.Context) (_ interface{}, rerr error) {
		snapshotID := sr.getSnapshotID()
		if _, err := sr.cm.Snapshotter.Stat(ctx, snapshotID); err == nil {
			return nil, nil
		}

		if sr.cm.Applier == nil {
			return nil, errors.New("extract requires an applier")
		}

		eg, egctx := errgroup.WithContext(ctx)

		parentID := ""
		if sr.parent != nil {
			eg.Go(func() error {
				if err := sr.parent.extract(egctx, dhs, s); err != nil {
					return err
				}
				parentID = sr.parent.getSnapshotID()
				return nil
			})
		}

		desc, err := sr.ociDesc()
		if err != nil {
			return nil, err
		}
		dh := dhs[desc.Digest]

		eg.Go(func() error {
			// unlazies if needed, otherwise a no-op
			return lazyRefProvider{
				ref:     sr,
				desc:    desc,
				dh:      dh,
				session: s,
			}.Unlazy(egctx)
		})

		if err := eg.Wait(); err != nil {
			return nil, err
		}

		if dh != nil && dh.Progress != nil {
			_, stopProgress := dh.Progress.Start(ctx)
			defer stopProgress(rerr)
			statusDone := dh.Progress.Status("extracting "+desc.Digest.String(), "extracting")
			defer statusDone()
		}

		key := fmt.Sprintf("extract-%s %s", identity.NewID(), sr.getChainID())

		err = sr.cm.Snapshotter.Prepare(ctx, key, parentID)
		if err != nil {
			return nil, err
		}

		mountable, err := sr.cm.Snapshotter.Mounts(ctx, key)
		if err != nil {
			return nil, err
		}
		mounts, unmount, err := mountable.Mount()
		if err != nil {
			return nil, err
		}
		_, err = sr.cm.Applier.Apply(ctx, desc, mounts)
		if err != nil {
			unmount()
			return nil, err
		}

		if err := unmount(); err != nil {
			return nil, err
		}
		if err := sr.cm.Snapshotter.Commit(ctx, sr.getSnapshotID(), key); err != nil {
			if !errors.Is(err, errdefs.ErrAlreadyExists) {
				return nil, err
			}
		}
		sr.queueBlobOnly(false)
		sr.queueSize(sizeUnknown)
		if err := sr.CommitMetadata(); err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

func (sr *ImmutableRef) ToImmutable(ctx context.Context) (*ImmutableRef, error) {
	return sr, nil
}

func (sr *ImmutableRef) Release(ctx context.Context) error {
	sr.cm.mu.Lock()
	defer sr.cm.mu.Unlock()

	sr.mu.Lock()
	defer sr.mu.Unlock()

	return sr.release(ctx)
}

// hold cacheRecord.mu lock before calling
func (sr *ImmutableRef) release(ctx context.Context) error {
	delete(sr.immutableRefs, sr)

	doUpdateLastUsed := sr.triggerLastUsed
	if doUpdateLastUsed {
		for r := range sr.immutableRefs {
			if r.triggerLastUsed {
				doUpdateLastUsed = false
				break
			}
		}
		if sr.mutableRef != nil && sr.mutableRef.triggerLastUsed {
			doUpdateLastUsed = false
		}
	}
	if doUpdateLastUsed {
		sr.queueLastUsed()
		sr.CommitMetadata()
		sr.triggerLastUsed = false
	}
	return sr.cacheRecord.release(ctx, true)
}

func (sr *ImmutableRef) commitLocked(ctx context.Context) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.commit(ctx)
}

// caller must hold cacheRecord.mu
func (sr *ImmutableRef) commit(ctx context.Context) error {
	if sr.isCommitted {
		return nil
	}
	if sr.mutableRef != nil {
		// can't commit the snapshot if someone still has an open mutable ref to it
		return errors.Wrap(ErrLocked, "cannot commit record with open mutable ref")
	}

	if err := sr.cm.ManagerOpt.LeaseManager.AddResource(ctx, leases.Lease{ID: sr.ID()}, leases.Resource{
		ID:   sr.ID(),
		Type: "snapshots/" + sr.cm.ManagerOpt.Snapshotter.Name(),
	}); err != nil {
		sr.cm.LeaseManager.Delete(context.TODO(), leases.Lease{ID: sr.ID()})
		return errors.Wrapf(err, "failed to add snapshot %s to lease", sr.ID())
	}

	if err := sr.cm.Snapshotter.Commit(ctx, sr.ID(), sr.getSnapshotID()); err != nil {
		sr.cm.LeaseManager.Delete(context.TODO(), leases.Lease{ID: sr.ID()})
		return errors.Wrapf(err, "failed to commit %s", sr.getSnapshotID())
	}

	sr.isCommitted = true
	sr.queueSnapshotID(sr.ID())

	// If there is a hard-crash here, the old snapshot id written to metadata is no longer valid, but
	// this situation is checked for and fixed in cacheManager.getRecord.

	return sr.CommitMetadata()
}

func (sr *MutableRef) Mount(ctx context.Context, readonly bool, s session.Group) (snapshot.Mountable, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.cm.Snapshotter.Name() == "stargz" && sr.parent != nil {
		var (
			m    snapshot.Mountable
			rerr error
		)
		if err := sr.parent.withRemoteSnapshotLabelsStargzMode(ctx, s, func() {
			m, rerr = sr.mount(ctx, readonly)
		}); err != nil {
			return nil, err
		}
		return m, rerr
	}

	return sr.mount(ctx, readonly)
}

// must be called holding cacheRecord mu
func (sr *MutableRef) mount(ctx context.Context, readonly bool) (snapshot.Mountable, error) {
	m, err := sr.cm.Snapshotter.Mounts(ctx, sr.getSnapshotID())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to mount %s", sr.ID())
	}
	if readonly {
		m = setReadonly(m)
	}
	return m, nil
}

func (sr *MutableRef) ToImmutable(ctx context.Context) (*ImmutableRef, error) {
	sr.cm.mu.Lock()
	defer sr.cm.mu.Unlock()

	sr.mu.Lock()
	defer sr.mu.Unlock()

	rec := sr.cacheRecord

	ir := &ImmutableRef{
		cacheRecord:  rec,
		descHandlers: sr.descHandlers,
	}
	rec.immutableRefs[ir] = struct{}{}

	if err := sr.release(ctx); err != nil {
		delete(rec.immutableRefs, ir)
		return nil, err
	}
	return ir, nil
}

func (sr *MutableRef) Release(ctx context.Context) error {
	sr.cm.mu.Lock()
	defer sr.cm.mu.Unlock()

	sr.mu.Lock()
	defer sr.mu.Unlock()

	return sr.release(ctx)
}

// hold cacheRecord.mu lock before calling
func (sr *MutableRef) release(ctx context.Context) error {
	sr.mutableRef = nil

	if sr.triggerLastUsed {
		sr.queueLastUsed()
		sr.CommitMetadata()
		sr.triggerLastUsed = false
	}
	return sr.cacheRecord.release(ctx, false)
}

func setReadonly(mounts snapshot.Mountable) snapshot.Mountable {
	return &readOnlyMounter{mounts}
}

type readOnlyMounter struct {
	snapshot.Mountable
}

func (m *readOnlyMounter) Mount() ([]mount.Mount, func() error, error) {
	mounts, release, err := m.Mountable.Mount()
	if err != nil {
		return nil, nil, err
	}
	for i, m := range mounts {
		if m.Type == "overlay" {
			mounts[i].Options = readonlyOverlay(m.Options)
			continue
		}
		opts := make([]string, 0, len(m.Options))
		for _, opt := range m.Options {
			if opt != "rw" {
				opts = append(opts, opt)
			}
		}
		opts = append(opts, "ro")
		mounts[i].Options = opts
	}
	return mounts, release, nil
}

func readonlyOverlay(opt []string) []string {
	out := make([]string, 0, len(opt))
	upper := ""
	for _, o := range opt {
		if strings.HasPrefix(o, "upperdir=") {
			upper = strings.TrimPrefix(o, "upperdir=")
		} else if !strings.HasPrefix(o, "workdir=") {
			out = append(out, o)
		}
	}
	if upper != "" {
		for i, o := range out {
			if strings.HasPrefix(o, "lowerdir=") {
				out[i] = "lowerdir=" + upper + ":" + strings.TrimPrefix(o, "lowerdir=")
			}
		}
	}
	return out
}
