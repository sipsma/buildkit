package cache

import (
	"context"
	"strings"
	"sync"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/snapshots"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/snapshot"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/compression"
	"github.com/moby/buildkit/util/flightcontrol"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

// Ref is a reference to cacheable objects.
type Ref interface {
	Mountable
	Metadata
	Release(context.Context) error
}

type ImmutableRef interface {
	Ref
	Parent() ImmutableRef
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
	cm     *cacheManager
	mu     sync.Mutex
	sizeG  flightcontrol.Group
	*storageItem

	parentRefs

	// immutableRefs keeps track of each ref pointing to this cacheRecord that can't change the underlying snapshot
	// data. When it's empty and mutableRef below is empty, that means there's no more unreleased pointers to this
	// struct and the cacheRecord can be considered for deletion. We enforce that there can not be immutableRefs while
	// mutableRef is set.
	immutableRefs map[*immutableRef]struct{}

	// mutableRef keeps track of a ref to this cacheRecord whose snapshot can be mounted read-write.
	// We enforce that at most one mutable ref points to this cacheRecord at a time and no immutableRefs
	// are set while mutableRef is set.
	mutableRef *mutableRef

	// isFinalized means the underlying snapshot has been committed to its driver
	isFinalized bool

	// dead means record is marked as deleted
	dead bool

	view      string
	viewMount snapshot.Mountable

	parentChainCache []digest.Digest
}

type parentRefs struct {
	// TODO doc that these are mutually exclusive
	layerParent  *immutableRef
	mergeParents []*immutableRef
}

// caller must hold cacheManager.mu
func (p parentRefs) release(ctx context.Context) error {
	if p.layerParent != nil {
		p.layerParent.mu.Lock()
		defer p.layerParent.mu.Unlock()
		return p.layerParent.release(ctx)
	}
	for _, parent := range p.mergeParents {
		parent.mu.Lock()
		if err := parent.release(ctx); err != nil {
			parent.mu.Unlock()
			return err
		}
		parent.mu.Unlock()
	}
	return nil
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

func (cr *cacheRecord) walkAncestors(f func(*cacheRecord) error) error {
	curs := []*cacheRecord{cr}
	for len(curs) > 0 {
		cur := curs[len(curs)-1]
		curs = curs[:len(curs)-1]
		if err := f(cur); err != nil {
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

func (cr *cacheRecord) walkUniqueAncestors(f func(*cacheRecord) error) error {
	memo := make(map[*cacheRecord]struct{})
	return cr.walkAncestors(func(cr *cacheRecord) error {
		if _, ok := memo[cr]; ok {
			return nil
		}
		memo[cr] = struct{}{}
		return f(cr)
	})
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
	// TODO fit in with snapshot.go?
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
		// TODO use cr.snapshotSize
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

func (sr *immutableRef) Clone() ImmutableRef {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.clone(false)
}

// hold cacheRecord.mu lock before calling
func (sr *immutableRef) clone(triggerLastUsed bool) ImmutableRef {
	ir2 := &immutableRef{
		cacheRecord:     sr.cacheRecord,
		triggerLastUsed: triggerLastUsed,
		descHandlers:    sr.descHandlers,
	}
	ir2.immutableRefs[ir2] = struct{}{}
	return ir2
}

func (sr *immutableRef) Parent() ImmutableRef {
	p := sr.parent
	if p == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	return p.clone(true)
}

func (sr *immutableRef) ociDesc() (ocispec.Descriptor, error) {
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

func (sr *immutableRef) getCompressionBlob(ctx context.Context, compressionType compression.Type) (content.Info, error) {
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

// order is from parent->child, sr will be at end of slice
func (sr *immutableRef) parentRefChain() []*immutableRef {
	var count int
	for ref := sr; ref != nil; ref = ref.parent {
		count++
	}
	refs := make([]*immutableRef, count)
	for i, ref := count-1, sr; ref != nil; i, ref = i-1, ref.parent {
		refs[i] = ref
	}
	return refs
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
		sr.CommitMetadata()
		sr.triggerLastUsed = false
	}
	return sr.cacheRecord.release(ctx, true)
}

func (sr *immutableRef) finalizeLocked(ctx context.Context) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.finalize(ctx)
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
		sr.CommitMetadata()
		sr.triggerLastUsed = false
	}
	return sr.cacheRecord.release(ctx, false)
}
