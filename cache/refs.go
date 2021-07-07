package cache

import (
	"context"
	"fmt"
	"sync"

	"github.com/containerd/containerd/leases"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/snapshot"
	"github.com/moby/buildkit/util/flightcontrol"
	"github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
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
	cm    *cacheManager
	mu    sync.Mutex
	sizeG flightcontrol.Group
	*cacheMetadata

	parentRefs

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

	mountCache snapshot.Mountable
}

type parentRefs struct {
	// TODO doc that these are mutually exclusive (sum type)
	layerParent  *ImmutableRef
	mergeParents []*ImmutableRef
}

// TODO add newParentRefs funcs so that you can enforce the sum type

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

// TODO new name
type refKind int

const (
	LAYER refKind = iota
	MERGE
)

var invalidRefKind = errors.New("unhandled ref kind")

func (p parentRefs) kind() refKind {
	// TODO double check you didn't inline len(p.mergeParents) > 0 anywhere
	if len(p.mergeParents) > 0 {
		return MERGE
	}
	return LAYER
}

// TODO do we need to deal with the fact that now a single blob can have multiple chainIDs? Technically could have
// happened before but is much more common now

func (p parentRefs) parentRecords() []*cacheRecord {
	var recs []*cacheRecord
	if p.layerParent != nil {
		recs = append(recs, p.layerParent.cacheRecord)
	}
	for _, parent := range p.mergeParents {
		recs = append(recs, parent.cacheRecord)
	}
	return recs
}

// TODO Change this to something like ForEachLayer(func(ref Ref) error) error
func (sr *ImmutableRef) Parent() *ImmutableRef {
	p := sr.layerParent
	if p == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	return p.clone(true)
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
func (cr *cacheRecord) mref(ctx context.Context, triggerLastUsed bool, descHandlers DescHandlers) (*MutableRef, error) {
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
	cr.mountCache = nil
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

// TODO put somewhere else
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

// TODO cache this
func (cr *cacheRecord) layerChain() (layers []*cacheRecord) {
	switch cr.kind() {
	case MERGE:
		for _, parent := range cr.mergeParents {
			layers = append(layers, parent.layerChain()...)
		}
		return layers
	case LAYER:
		if cr.layerParent != nil {
			layers = append(layers, cr.layerParent.layerChain()...)
		}
		layers = append(layers, cr)
		return layers
	}
	return nil // TODO
}

func (cr *cacheRecord) layerDigestChain() []digest.Digest {
	var dgsts []digest.Digest
	for _, layer := range cr.layerChain() {
		dgst := layer.getBlob()
		if dgst == "" {
			return nil
		}
		dgsts = append(dgsts, dgst)
	}
	return dgsts
}

// call when holding the manager lock
func (cr *cacheRecord) remove(ctx context.Context) error {
	delete(cr.cm.records, cr.ID())
	if err := cr.cm.store.Clear(cr.ID()); err != nil {
		return err
	}
	if err := cr.cm.Snapshotter.RemoveLease(leases.WithLease(ctx, cr.ID()), cr.getSnapshotID()); err != nil {
		return errors.Wrapf(err, "failed to remove %s", cr.ID())
	}
	if err := cr.parentRefs.release(ctx); err != nil {
		return err
	}
	return nil
}

// hold cacheRecord.mu lock before calling
func (cr *cacheRecord) release(ctx context.Context, forceKeep bool) error {
	if cr.refCount() > 0 {
		return nil
	}

	// TODO is this really needed? What's the downside of keeping the View lease open?
	if err := cr.cm.Snapshotter.RemoveViewLease(leases.WithLease(ctx, cr.ID()), cr.getSnapshotID()); err != nil {
		return errors.Wrapf(err, "failed to remove view lease of %s", cr.getSnapshotID())
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

func (sr *ImmutableRef) ToImmutable(ctx context.Context) (*ImmutableRef, error) {
	return sr, nil
}

type ErrNotMutable struct {
	error
}

func (e ErrNotMutable) Error() string {
	if errors.As(e.error, &ErrNotMutable{}) {
		return e.error.Error()
	}
	return fmt.Sprintf("ref cannot be made mutable: %v", e.error)
}

func (e ErrNotMutable) Unwrap() error {
	return e.error
}

// TODO put comment on interface
// Okay to continue using sr if errors.As(err, &ErrNotMutable{}), otherwise it's invalid (should still call release tho?)
/* TODO delete
func (sr *ImmutableRef) ToMutable(ctx context.Context) (*MutableRef, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	rec := sr.cacheRecord
	delete(rec.immutableRefs, sr)

	mr, err := rec.mref(ctx, false, sr.descHandlers)
	if err != nil {
		rec.immutableRefs[sr] = struct{}{}
		return nil, ErrNotMutable{err}
	}

	sr.cm.mu.Lock()
	defer sr.cm.mu.Unlock()

	if err := sr.release(ctx); err != nil {
		return nil, err
	}
	return mr, nil
}
*/

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
	}
	if doUpdateLastUsed {
		sr.queueLastUsed()
		sr.commitMetadata()
		sr.triggerLastUsed = false
	}
	return sr.cacheRecord.release(ctx, true)
}

// TODO put comment on interface
// sr is invalid if an error is returned, can still call release though?
func (sr *MutableRef) ToImmutable(ctx context.Context) (*ImmutableRef, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	rec := sr.cacheRecord

	ir := &ImmutableRef{
		cacheRecord:  rec,
		descHandlers: sr.descHandlers,
	}
	rec.immutableRefs[ir] = struct{}{}

	sr.cm.mu.Lock()
	defer sr.cm.mu.Unlock()

	if err := sr.release(ctx); err != nil {
		delete(rec.immutableRefs, ir)
		return nil, err
	}
	return ir, nil
}

/* TODO delete
func (sr *MutableRef) ToMutable(ctx context.Context) (*MutableRef, error) {
	return sr, nil
}
*/

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
		sr.commitMetadata()
		sr.triggerLastUsed = false
	}
	return sr.cacheRecord.release(ctx, false)
}
