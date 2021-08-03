package cache

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/snapshot"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/compression"
	"github.com/moby/buildkit/util/flightcontrol"
	"github.com/moby/buildkit/util/leaseutil"
	"github.com/moby/buildkit/util/winlayers"
	"github.com/opencontainers/go-digest"
	imagespecidentity "github.com/opencontainers/image-spec/identity"
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
}

type ImmutableRef interface {
	Ref
	Clone() ImmutableRef

	Extract(ctx context.Context, s session.Group) error // +progress
	GetRemote(ctx context.Context, createIfNeeded bool, compressionType compression.Type, forceCompression bool, s session.Group) (*solver.Remote, error)
}

type MutableRef interface {
	Ref
	Commit(context.Context) (ImmutableRef, error)
}

type Mountable interface {
	Mount(ctx context.Context, readonly bool, s session.Group) (snapshot.Mountable, error)
}

type cacheRecord struct {
	cm    *cacheManager
	mu    sync.Mutex
	sizeG flightcontrol.Group
	parentRefs
	*cacheMetadata

	// immutableRefs keeps track of each ref pointing to this cacheRecord that can't change the underlying snapshot
	// data. When it's empty and mutableRef below is empty, that means there's no more unreleased pointers to this
	// struct and the cacheRecord can be considered for deletion. We enforce that there can not be immutableRefs while
	// mutableRef is set.
	immutableRefs map[*immutableRef]struct{}

	// mutableRef keeps track of a ref to this cacheRecord whose snapshot can be mounted read-write.
	// We enforce that at most one mutable ref points to this cacheRecord at a time and no immutableRefs
	// are set while mutableRef is set.
	mutableRef *mutableRef

	// isFinalized means the underlying snapshot has been committed to its driver and cannot have an open mutable ref
	// pointing to it again
	isFinalized bool

	// dead means record is marked as deleted
	dead bool

	mountCache snapshot.Mountable

	layerDigestChainCache []digest.Digest
}

type immutableRef struct {
	*cacheRecord
	triggerLastUsed bool
	descHandlers    DescHandlers
}

var _ ImmutableRef = &immutableRef{}

type mutableRef struct {
	*cacheRecord
	triggerLastUsed bool
	descHandlers    DescHandlers
}

var _ MutableRef = &mutableRef{}

type parentKind int

const (
	None parentKind = iota
	Layer
	Merge
)

type parentRefs struct {
	// layerParent and mergeParents are mutually exclusive, at most one at a time should be set non-nil
	layerParent  *immutableRef
	mergeParents []*immutableRef
}

func (p parentRefs) parentKind() parentKind {
	if len(p.mergeParents) > 0 {
		return Merge
	}
	if p.layerParent != nil {
		return Layer
	}
	return None
}

// caller must hold cacheManager.mu
func (p parentRefs) release(ctx context.Context) error {
	switch p.parentKind() {
	case Layer:
		p.layerParent.mu.Lock()
		defer p.layerParent.mu.Unlock()
		return p.layerParent.release(ctx)
	case Merge:
		for _, parent := range p.mergeParents {
			parent.mu.Lock()
			if err := parent.release(ctx); err != nil {
				parent.mu.Unlock()
				return err
			}
			parent.mu.Unlock()
		}
	}
	return nil
}

func (p parentRefs) clone() parentRefs {
	switch p.parentKind() {
	case Layer:
		p.layerParent = p.layerParent.clone(false)
	case Merge:
		var newParents []*immutableRef
		for _, p := range p.mergeParents {
			newParents = append(newParents, p.clone(false))
		}
		p.mergeParents = newParents
	}
	return p
}

func (p parentRefs) mergeDigest() digest.Digest {
	switch p.parentKind() {
	case Merge:
		var mergeDigests []digest.Digest
		for _, parent := range p.mergeParents {
			if chainID := parent.getChainID(); chainID != "" {
				mergeDigests = append(mergeDigests, chainID)
			} else {
				mergeDigests = append(mergeDigests, digest.Digest(parent.ID()))
			}
		}
		return imagespecidentity.ChainID(mergeDigests)
	default:
		return ""
	}
}

func (p parentRefs) areFinalized() bool {
	switch p.parentKind() {
	case Layer:
		return p.layerParent.isFinalized
	case Merge:
		for _, p := range p.mergeParents {
			if !p.isFinalized {
				return false
			}
		}
		return true
	default:
		return true
	}
}

// hold cacheRecord.mu before calling
func (cr *cacheRecord) ref(triggerLastUsed bool, descHandlers DescHandlers) (*immutableRef, error) {
	if cr.mutableRef != nil {
		return nil, ErrLocked
	}

	r := &immutableRef{
		cacheRecord:     cr,
		triggerLastUsed: triggerLastUsed,
		descHandlers:    descHandlers,
	}
	cr.immutableRefs[r] = struct{}{}

	return r, nil
}

// hold cacheRecord.mu lock before calling
func (cr *cacheRecord) mref(triggerLastUsed bool, descHandlers DescHandlers) (*mutableRef, error) {
	if cr.isFinalized {
		return nil, errors.Wrap(errInvalid, "cannot get mutable ref of finalized cache record")
	}
	if cr.mutableRef != nil {
		return nil, ErrLocked
	}
	if len(cr.immutableRefs) > 0 {
		return nil, ErrLocked
	}

	r := &mutableRef{
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

// order is from parent->child, cr will be at end of slice
func (cr *cacheRecord) layerChain() (layers []*cacheRecord) {
	switch cr.parentKind() {
	case Merge:
		for _, parent := range cr.mergeParents {
			layers = append(layers, parent.layerChain()...)
		}
		return layers
	case Layer:
		layers = append(layers, cr.layerParent.layerChain()...)
		fallthrough
	case None:
		layers = append(layers, cr)
		return layers
	}
	return nil
}

// hold cacheRecord.mu lock before calling
func (cr *cacheRecord) layerDigestChain() []digest.Digest {
	if cr.layerDigestChainCache != nil {
		return cr.layerDigestChainCache
	}
	var dgsts []digest.Digest
	for _, layer := range cr.layerChain() {
		dgst := layer.getBlob()
		if dgst == "" {
			return nil
		}
		dgsts = append(dgsts, dgst)
	}
	cr.layerDigestChainCache = dgsts
	return dgsts
}

var skipWalk = errors.New("skip")

func (cr *cacheRecord) walkRecords(f func(*cacheRecord) error) error {
	curs := []*cacheRecord{cr}
	for len(curs) > 0 {
		cur := curs[len(curs)-1]
		curs = curs[:len(curs)-1]
		if err := f(cur); err != nil {
			if errors.Is(err, skipWalk) {
				continue
			}
			return err
		}
		if cur.layerParent != nil {
			curs = append(curs, cur.layerParent.cacheRecord)
		}
		for _, p := range cur.mergeParents {
			curs = append(curs, p.cacheRecord)
		}
	}
	return nil
}

func (cr *cacheRecord) walkUniqueRecords(f func(*cacheRecord) error) error {
	memo := make(map[*cacheRecord]struct{})
	return cr.walkRecords(func(cr *cacheRecord) error {
		if _, ok := memo[cr]; ok {
			return skipWalk
		}
		memo[cr] = struct{}{}
		return f(cr)
	})
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
		var totalSize int64
		if !cr.getBlobOnly() {
			if usage, err := cr.cm.ManagerOpt.Snapshotter.Usage(ctx, driverID); err != nil {
				cr.mu.Lock()
				isDead := cr.dead
				cr.mu.Unlock()
				if isDead {
					return int64(0), nil
				}
				if !errors.Is(err, errdefs.ErrNotFound) {
					return s, errors.Wrapf(err, "failed to get usage for %s", cr.ID())
				}
			} else {
				totalSize += usage.Size
			}
		}
		if dgst := cr.getBlob(); dgst != "" {
			info, err := cr.cm.ContentStore.Info(ctx, digest.Digest(dgst))
			if err == nil {
				totalSize += info.Size
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
							totalSize += info.Size
						}
					}
				}
			}
		}
		cr.mu.Lock()
		cr.queueSize(totalSize)
		if err := cr.commitMetadata(); err != nil {
			cr.mu.Unlock()
			return s, err
		}
		cr.mu.Unlock()
		return totalSize, nil
	})
	if err != nil {
		return 0, err
	}
	return s.(int64), nil
}

// call when holding the manager lock
func (cr *cacheRecord) remove(ctx context.Context) error {
	delete(cr.cm.records, cr.ID())
	if err := cr.cm.Snapshotter.RemoveLease(context.TODO(), cr.ID(), cr.getSnapshotID()); err != nil {
		return errors.Wrapf(err, "failed to remove snapshot lease for %s", cr.ID())
	}
	if err := cr.cm.LeaseManager.Delete(ctx, leases.Lease{
		ID: cr.ID(),
	}); err != nil {
		return errors.Wrapf(err, "failed to delete lease for %s", cr.ID())
	}
	if err := cr.cm.store.Clear(cr.ID()); err != nil {
		return errors.Wrapf(err, "failed to delete metadata of %s", cr.ID())
	}
	if err := cr.parentRefs.release(ctx); err != nil {
		return errors.Wrapf(err, "failed to release parents of %s", cr.ID())
	}
	return nil
}

// hold cacheRecord.mu and cacheManager.mu locks before calling
func (cr *cacheRecord) release(ctx context.Context, forceKeep bool) error {
	if cr.refCount() > 0 {
		return nil
	}

	if forceKeep || cr.HasCachePolicyRetain() {
		return nil
	}
	return cr.remove(ctx)
}

func (sr *immutableRef) Clone() ImmutableRef {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.clone(false)
}

// hold cacheRecord.mu lock before calling
func (sr *immutableRef) clone(triggerLastUsed bool) *immutableRef {
	ir2 := &immutableRef{
		cacheRecord:     sr.cacheRecord,
		triggerLastUsed: triggerLastUsed,
		descHandlers:    sr.descHandlers,
	}
	ir2.immutableRefs[ir2] = struct{}{}
	return ir2
}

func (cr *cacheRecord) ociDesc() (ocispec.Descriptor, error) {
	dgst := cr.getBlob()
	if dgst == "" {
		return ocispec.Descriptor{}, errors.Errorf("no blob set for cache record %s", cr.ID())
	}

	desc := ocispec.Descriptor{
		Digest:      cr.getBlob(),
		Size:        cr.getBlobSize(),
		MediaType:   cr.getMediaType(),
		Annotations: make(map[string]string),
	}

	diffID := cr.getDiffID()
	if diffID != "" {
		desc.Annotations["containerd.io/uncompressed"] = string(diffID)
	}

	createdAt := cr.GetCreatedAt()
	if !createdAt.IsZero() {
		createdAt, err := createdAt.MarshalText()
		if err != nil {
			return ocispec.Descriptor{}, err
		}
		desc.Annotations["buildkit/createdat"] = string(createdAt)
	}

	if description := cr.GetDescription(); description != "" {
		desc.Annotations["buildkit/description"] = description
	}

	return desc, nil
}

const compressionVariantDigestLabelPrefix = "buildkit.io/compression/digest."

func compressionVariantDigestLabel(compressionType compression.Type) string {
	return compressionVariantDigestLabelPrefix + compressionType.String()
}

func (cr *cacheRecord) getCompressionBlob(ctx context.Context, compressionType compression.Type) (content.Info, error) {
	cs := cr.cm.ContentStore
	info, err := cs.Info(ctx, cr.getBlob())
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

func (sr *immutableRef) addCompressionBlob(ctx context.Context, dgst digest.Digest, compressionType compression.Type) error {
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

func (sr *immutableRef) Mount(ctx context.Context, readonly bool, s session.Group) (snapshot.Mountable, error) {
	return sr.cacheRecord.Mount(ctx, true, sr.descHandlers, s)
}

func (sr *immutableRef) Extract(ctx context.Context, s session.Group) (rerr error) {
	if !sr.getBlobOnly() {
		return
	}
	if sr.cm.Applier == nil {
		return errors.New("extract requires an applier")
	}
	return sr.cacheRecord.PrepareMount(ctx, sr.descHandlers, s)
}

func (cr *cacheRecord) Mount(ctx context.Context, readonly bool, dhs DescHandlers, s session.Group) (snapshot.Mountable, error) {
	if err := cr.PrepareMount(ctx, dhs, s); err != nil {
		return nil, err
	}
	mnt := cr.mountCache
	if readonly {
		mnt = setReadonly(mnt)
	}

	return mnt, nil
}

func (cr *cacheRecord) PrepareMount(ctx context.Context, dhs DescHandlers, s session.Group) (rerr error) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if cr.mountCache != nil {
		return nil
	}

	ctx, done, err := leaseutil.WithLease(ctx, cr.cm.LeaseManager, leaseutil.MakeTemporary)
	if err != nil {
		return err
	}
	defer done(ctx)

	if cr.GetLayerType() == "windows" {
		ctx = winlayers.UseWindowsLayerMode(ctx)
	}

	if cr.cm.Snapshotter.Name() == "stargz" {
		if err := cr.withRemoteSnapshotLabelsStargzMode(ctx, dhs, s, func() {
			if rerr = cr.prepareRemoteSnapshotsStargzMode(ctx, dhs, s); rerr != nil {
				return
			}
			rerr = cr.prepareMount(ctx, dhs, s)
		}); err != nil {
			return err
		}
		return rerr
	}

	return cr.prepareMount(ctx, dhs, s)
}

func (cr *cacheRecord) withRemoteSnapshotLabelsStargzMode(ctx context.Context, dhs DescHandlers, s session.Group, f func()) error {
	for _, r := range cr.layerChain() {
		r := r
		info, err := r.cm.Snapshotter.Stat(ctx, r.getSnapshotID())
		if err != nil && !errdefs.IsNotFound(err) {
			return err
		} else if errdefs.IsNotFound(err) {
			continue // This snpashot doesn't exist; skip
		} else if _, ok := info.Labels["containerd.io/snapshot/remote"]; !ok {
			continue // This isn't a remote snapshot; skip
		}
		dh := dhs[r.getBlob()]
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

func (cr *cacheRecord) prepareRemoteSnapshotsStargzMode(ctx context.Context, dhs DescHandlers, s session.Group) error {
	_, err := cr.sizeG.Do(ctx, cr.ID()+"-prepare-remote-snapshot", func(ctx context.Context) (_ interface{}, rerr error) {
		for _, r := range cr.layerChain() {
			r := r
			snapshotID := r.getSnapshotID()
			if _, err := r.cm.Snapshotter.Stat(ctx, snapshotID); err == nil {
				continue
			}

			dh := dhs[r.getBlob()]
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
			if r.layerParent != nil {
				parentID = r.layerParent.getSnapshotID()
			}
			if err := r.cm.Snapshotter.Prepare(ctx, key, parentID, opts...); err != nil {
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

func (cr *cacheRecord) prepareMount(ctx context.Context, dhs DescHandlers, s session.Group) error {
	_, err := cr.sizeG.Do(ctx, cr.ID()+"-extract", func(ctx context.Context) (_ interface{}, rerr error) {
		if cr.mountCache != nil {
			return nil, nil
		}

		eg, egctx := errgroup.WithContext(ctx)

		var parentSnapshotIDs []string
		switch cr.parentKind() {
		case Layer:
			eg.Go(func() error {
				parentSnapshotIDs = append(parentSnapshotIDs, cr.layerParent.getSnapshotID())
				if err := cr.layerParent.prepareMount(egctx, dhs, s); err != nil {
					return err
				}
				return nil
			})
		case Merge:
			for _, parent := range cr.mergeParents {
				parent := parent
				parentSnapshotIDs = append(parentSnapshotIDs, parent.getSnapshotID())
				eg.Go(func() error {
					if err := parent.prepareMount(egctx, dhs, s); err != nil {
						return err
					}
					return nil
				})
			}
		case None:
			// "parent" is just an empty snapshot
			parentSnapshotIDs = append(parentSnapshotIDs, "")
		}

		var dh *DescHandler
		var desc ocispec.Descriptor
		if cr.getBlobOnly() {
			var err error
			desc, err = cr.ociDesc()
			if err != nil {
				return nil, err
			}
			dh = dhs[desc.Digest]

			eg.Go(func() error {
				// unlazies if needed, otherwise a no-op
				return lazyRefProvider{
					rec:     cr,
					desc:    desc,
					dh:      dh,
					session: s,
				}.Unlazy(egctx)
			})
		}

		if err := eg.Wait(); err != nil {
			return nil, err
		}

		if cr.getBlobOnly() {
			if dh != nil && dh.Progress != nil {
				_, stopProgress := dh.Progress.Start(ctx)
				defer stopProgress(rerr)
				statusDone := dh.Progress.Status("extracting "+desc.Digest.String(), "extracting")
				defer statusDone()
			}

			if err := cr.cm.Snapshotter.Prepare(ctx, cr.getSnapshotID(), parentSnapshotIDs[0]); err != nil {
				return nil, err
			}

			mountable, err := cr.cm.Snapshotter.Mounts(ctx, cr.getSnapshotID())
			if err != nil {
				return nil, err
			}
			mounts, unmount, err := mountable.Mount()
			if err != nil {
				return nil, err
			}
			_, err = cr.cm.Applier.Apply(ctx, desc, mounts)
			if err != nil {
				unmount()
				return nil, err
			}

			if err := unmount(); err != nil {
				return nil, err
			}
			if err := cr.cm.Snapshotter.Commit(ctx, cr.getSnapshotID()); err != nil {
				if !errors.Is(err, errdefs.ErrAlreadyExists) {
					return nil, err
				}
			}
			cr.queueBlobOnly(false)
			cr.queueSize(sizeUnknown)
			if err := cr.commitMetadata(); err != nil {
				return nil, err
			}
		}

		if cr.parentKind() == Merge {
			if err := cr.cm.Snapshotter.Merge(ctx, cr.getSnapshotID(), parentSnapshotIDs); err != nil {
				return nil, err
			}
		}

		mountable, err := cr.cm.Snapshotter.Mounts(leases.WithLease(ctx, cr.ID()), cr.getSnapshotID())
		if err != nil {
			return nil, err
		}
		cr.mountCache = mountable
		return nil, nil
	})
	return err
}

func (sr *immutableRef) Release(ctx context.Context) error {
	sr.cm.mu.Lock()
	defer sr.cm.mu.Unlock()

	sr.mu.Lock()
	defer sr.mu.Unlock()

	return sr.release(ctx)
}

// hold cacheRecord.mu lock before calling
func (sr *immutableRef) release(ctx context.Context) error {
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
		sr.commitMetadata()
		sr.triggerLastUsed = false
	}
	return sr.cacheRecord.release(ctx, true)
}

func (sr *immutableRef) finalizeLocked(ctx context.Context) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.finalize(ctx)
}

// caller must hold cacheRecord.mu
func (sr *immutableRef) finalize(ctx context.Context) error {
	if sr.isFinalized {
		return nil
	}
	if sr.mutableRef != nil {
		// can't commit the snapshot if someone still has an open mutable ref to it
		return errors.Wrap(ErrLocked, "cannot finalize record with open mutable ref")
	}

	switch sr.parentKind() {
	case Layer:
		if err := sr.layerParent.finalizeLocked(ctx); err != nil {
			return errors.Wrapf(err, "failed to finalize parent ref %q", sr.layerParent.ID())
		}
		fallthrough
	case None:
		if err := sr.cm.Snapshotter.Commit(ctx, sr.getSnapshotID()); err != nil {
			sr.cm.LeaseManager.Delete(context.TODO(), leases.Lease{ID: sr.ID()})
			return errors.Wrapf(err, "failed to commit %s", sr.getSnapshotID())
		}
	case Merge:
		for _, parent := range sr.mergeParents {
			if err := parent.finalizeLocked(ctx); err != nil {
				return errors.Wrapf(err, "failed to finalize parent ref %q of merge ref", parent.ID())
			}
		}
		mergeDigest := sr.mergeDigest()
		sr.queueMergeDigest(mergeDigest)
		if err := sr.commitMetadata(); err != nil {
			return errors.Wrapf(err, "failed to commit merge digest")
		}
		// TODO(sipsma) optimize by re-using an equal merge ref if multiple are equal after calculating the merge digest
	}

	sr.isFinalized = true
	return nil
}

func (sr *mutableRef) Mount(ctx context.Context, readonly bool, s session.Group) (snapshot.Mountable, error) {
	return sr.cacheRecord.Mount(ctx, readonly, sr.descHandlers, s)
}

func (sr *mutableRef) Commit(ctx context.Context) (ImmutableRef, error) {
	sr.cm.mu.Lock()
	defer sr.cm.mu.Unlock()

	sr.mu.Lock()
	defer sr.mu.Unlock()

	rec := sr.cacheRecord

	ir := &immutableRef{
		cacheRecord:  rec,
		descHandlers: sr.descHandlers,
	}
	rec.immutableRefs[ir] = struct{}{}

	if err := sr.release(ctx); err != nil {
		delete(rec.immutableRefs, ir)
		return nil, err
	}

	sr.queueCommitted(true)
	if err := sr.commitMetadata(); err != nil {
		return nil, err
	}
	return ir, nil
}

func (sr *mutableRef) Release(ctx context.Context) error {
	sr.cm.mu.Lock()
	defer sr.cm.mu.Unlock()

	sr.mu.Lock()
	defer sr.mu.Unlock()

	return sr.release(ctx)
}

// hold cacheRecord.mu lock before calling
func (sr *mutableRef) release(ctx context.Context) error {
	sr.mutableRef = nil

	if sr.triggerLastUsed {
		sr.queueLastUsed()
		sr.commitMetadata()
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
