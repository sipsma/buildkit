package cache

import (
	"context"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/diff"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/filters"
	"github.com/containerd/containerd/gc"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/snapshots"
	"github.com/docker/docker/pkg/idtools"
	"github.com/moby/buildkit/cache/metadata"
	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/snapshot"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/flightcontrol"
	"github.com/opencontainers/go-digest"
	imagespecidentity "github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

var (
	ErrLocked   = errors.New("locked")
	errNotFound = errors.New("not found")
	errInvalid  = errors.New("invalid")
)

type ManagerOpt struct {
	Snapshotter       snapshot.Snapshotter
	ContentStore      content.Store
	LeaseManager      leases.Manager
	PruneRefChecker   ExternalRefCheckerFunc
	GarbageCollect    func(ctx context.Context) (gc.Stats, error)
	Applier           diff.Applier
	Differ            diff.Comparer
	MetadataStoreRoot string
}

type Accessor interface {
	MetadataStore

	GetByBlob(ctx context.Context, desc ocispec.Descriptor, parent ImmutableRef, opts ...RefOption) (ImmutableRef, error)
	Get(ctx context.Context, id string, opts ...RefOption) (ImmutableRef, error)

	New(ctx context.Context, parent ImmutableRef, s session.Group, opts ...RefOption) (MutableRef, error)
	// GetMutable returns a mutable reference to the cache record with the given ID if it is possible to do so. If the
	// provided cache record ID points to a committed but not finalized record, than a new cache record will be returned
	// that is a copy of the previous but with a new ID (in order to invalidate any references to the previous ID).
	GetMutable(ctx context.Context, id string, opts ...RefOption) (MutableRef, error) // Rebase?
	IdentityMapping() *idtools.IdentityMapping
	Merge(ctx context.Context, parents []ImmutableRef, opts ...RefOption) (ImmutableRef, error)
}

type Controller interface {
	DiskUsage(ctx context.Context, info client.DiskUsageInfo) ([]*client.UsageInfo, error)
	Prune(ctx context.Context, ch chan client.UsageInfo, info ...client.PruneInfo) error
}

type Manager interface {
	Accessor
	Controller
	Close() error
}

type ExternalRefCheckerFunc func() (ExternalRefChecker, error)

type ExternalRefChecker interface {
	Exists(string, []digest.Digest) bool
}

type cacheManager struct {
	records map[string]*cacheRecord
	mu      sync.Mutex
	ManagerOpt
	store *metadata.Store

	muPrune sync.Mutex // make sure parallel prune is not allowed so there will not be inconsistent results
	unlazyG flightcontrol.Group
}

func NewManager(opt ManagerOpt) (Manager, error) {
	if err := MigrateV2(
		context.TODO(),
		filepath.Join(opt.MetadataStoreRoot, "metadata.db"),
		filepath.Join(opt.MetadataStoreRoot, "metadata_v2.db"),
		opt.ContentStore,
		opt.Snapshotter,
		opt.LeaseManager,
	); err != nil {
		return nil, err
	}

	md, err := metadata.NewStore(filepath.Join(opt.MetadataStoreRoot, "metadata_v2.db"))
	if err != nil {
		return nil, err
	}

	cm := &cacheManager{
		ManagerOpt: opt,
		store:      md,
		records:    make(map[string]*cacheRecord),
	}

	if err := cm.init(context.TODO()); err != nil {
		return nil, err
	}

	// cm.scheduleGC(5 * time.Minute)

	return cm, nil
}

func (cm *cacheManager) GetByBlob(ctx context.Context, desc ocispec.Descriptor, parent ImmutableRef, opts ...RefOption) (ir ImmutableRef, rerr error) {
	diffID, err := diffIDFromDescriptor(desc)
	if err != nil {
		return nil, err
	}
	chainID := diffID
	blobChainID := imagespecidentity.ChainID([]digest.Digest{desc.Digest, diffID})

	descHandlers := descHandlersOf(opts...)
	if desc.Digest != "" && (descHandlers == nil || descHandlers[desc.Digest] == nil) {
		if _, err := cm.ContentStore.Info(ctx, desc.Digest); errors.Is(err, errdefs.ErrNotFound) {
			return nil, NeedsRemoteProvidersError([]digest.Digest{desc.Digest})
		} else if err != nil {
			return nil, err
		}
	}

	var p *immutableRef
	if parent != nil {
		p2, err := cm.Get(ctx, parent.ID(), NoUpdateLastUsed, descHandlers)
		if err != nil {
			return nil, err
		}
		p = p2.(*immutableRef)

		if err := p.finalizeLocked(ctx); err != nil {
			p.Release(context.TODO())
			return nil, err
		}

		if p.getChainID() == "" || p.getBlobChainID() == "" {
			p.Release(context.TODO())
			return nil, errors.Errorf("failed to get ref by blob on non-addressable parent")
		}
		chainID = imagespecidentity.ChainID([]digest.Digest{p.getChainID(), chainID})
		blobChainID = imagespecidentity.ChainID([]digest.Digest{p.getBlobChainID(), blobChainID})
	}

	releaseParent := false
	defer func() {
		if releaseParent || rerr != nil && p != nil {
			p.Release(context.TODO())
		}
	}()

	cm.mu.Lock()
	defer cm.mu.Unlock()

	sis, err := cm.searchBlobchain(ctx, blobChainID)
	if err != nil {
		return nil, err
	}

	for _, si := range sis {
		ref, err := cm.get(ctx, si.ID(), opts...)
		if err != nil && !IsNotFound(err) {
			return nil, errors.Wrapf(err, "failed to get record %s by blobchainid", sis[0].ID())
		}
		if ref == nil {
			continue
		}
		if p != nil {
			releaseParent = true
		}
		if err := setImageRefMetadata(ref.cacheMetadata, opts...); err != nil {
			return nil, errors.Wrapf(err, "failed to append image ref metadata to ref %s", ref.ID())
		}
		return ref, nil
	}

	sis, err = cm.searchChain(ctx, chainID)
	if err != nil {
		return nil, err
	}

	var link *immutableRef
	for _, si := range sis {
		ref, err := cm.get(ctx, si.ID(), opts...)
		// if the error was NotFound or NeedsRemoteProvider, we can't re-use the snapshot from the blob so just skip it
		if err != nil && !IsNotFound(err) && !errors.As(err, &NeedsRemoteProvidersError{}) {
			return nil, errors.Wrapf(err, "failed to get record %s by chainid", si.ID())
		}
		if ref != nil {
			link = ref
			break
		}
	}

	id := identity.NewID()
	snapshotID := chainID.String()
	blobOnly := true
	if link != nil {
		snapshotID = link.getSnapshotID()
		blobOnly = link.getBlobOnly()
		go link.Release(context.TODO())
	}

	l, err := cm.ManagerOpt.LeaseManager.Create(ctx, func(l *leases.Lease) error {
		l.ID = id
		l.Labels = map[string]string{
			"containerd.io/gc.flat": time.Now().UTC().Format(time.RFC3339Nano),
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create lease")
	}

	defer func() {
		if rerr != nil {
			if err := cm.ManagerOpt.LeaseManager.Delete(context.TODO(), leases.Lease{
				ID: l.ID,
			}); err != nil {
				logrus.Errorf("failed to remove lease: %+v", err)
			}
		}
	}()

	if err := cm.Snapshotter.AddLease(ctx, l.ID, snapshotID); err != nil {
		return nil, errors.Wrapf(err, "failed to add snapshot %s to lease", id)
	}

	if desc.Digest != "" {
		if err := cm.ManagerOpt.LeaseManager.AddResource(ctx, leases.Lease{ID: id}, leases.Resource{
			ID:   desc.Digest.String(),
			Type: "content",
		}); err != nil {
			return nil, errors.Wrapf(err, "failed to add blob %s to lease", id)
		}
	}

	md, _ := cm.getMetadata(id)

	rec := &cacheRecord{
		cm:            cm,
		immutableRefs: make(map[*immutableRef]struct{}),
		parentRefs:    parentRefs{layerParent: p},
		cacheMetadata: md,
		isFinalized:   true,
	}

	if err := initializeMetadata(rec.cacheMetadata, rec.parentRefs, opts...); err != nil {
		return nil, err
	}

	if err := setImageRefMetadata(rec.cacheMetadata, opts...); err != nil {
		return nil, errors.Wrapf(err, "failed to append image ref metadata to ref %s", rec.ID())
	}

	rec.queueDiffID(diffID)
	rec.queueBlob(desc.Digest)
	rec.queueChainID(chainID)
	rec.queueBlobChainID(blobChainID)
	rec.queueSnapshotID(snapshotID)
	rec.queueBlobOnly(blobOnly)
	rec.queueMediaType(desc.MediaType)
	rec.queueBlobSize(desc.Size)
	rec.queueCommitted(true)

	if err := rec.commitMetadata(); err != nil {
		return nil, err
	}

	cm.records[id] = rec

	return rec.ref(true, descHandlers)
}

// init loads all snapshots from metadata state and tries to load the records
// from the snapshotter. If snaphot can't be found, metadata is deleted as well.
func (cm *cacheManager) init(ctx context.Context) error {
	items, err := cm.store.All()
	if err != nil {
		return err
	}

	toRemove := make(map[string]bool)
	for _, si := range items {
		md := &cacheMetadata{si}
		// Migrate any equalMutable/equalImmutable refs from older buildkit versions to using just a single ref
		if em := md.getEqualMutable(); em != "" {
			if emMd, exists := cm.getMetadata(em); exists {
				toRemove[em] = true
				if err := emMd.copyValuesTo(md); err != nil {
					logrus.Debugf("failed to migrate metadata from %s to %s", emMd.ID(), md.ID())
					toRemove[md.ID()] = true
					continue
				}
				md.clearEqualMutable()
				md.queueSize(sizeUnknown)
				_, err := cm.ManagerOpt.LeaseManager.Create(ctx, func(l *leases.Lease) error {
					l.ID = md.ID()
					l.Labels = map[string]string{
						"containerd.io/gc.flat": time.Now().UTC().Format(time.RFC3339Nano),
					}
					return nil
				})
				if err != nil && !errors.Is(err, errdefs.ErrAlreadyExists) {
					logrus.Debugf("failed to create lease for ref %s: %+v", md.ID(), err)
					toRemove[md.ID()] = true
					continue
				}
				if err := cm.Snapshotter.AddLease(ctx, md.ID(), emMd.ID()); err != nil {
					logrus.Debugf("failed to add snapshot %s to lease: %+v", emMd.ID(), err)
					toRemove[md.ID()] = true
					continue
				}
				if err := md.commitMetadata(); err != nil {
					toRemove[md.ID()] = true
					logrus.Debugf("failed to migrate equalMutable %s to ref %s: %+v", em, md.ID(), err)
					continue
				}
			}
		}
	}

	for _, si := range items {
		doRemove := toRemove[si.ID()]
		// don't call getRecord on items we're going to remove anyways as it causes them to get cached in cm.records
		if !doRemove {
			if _, err := cm.getRecord(ctx, si.ID()); err != nil {
				logrus.Debugf("could not load ref %s: %+v", si.ID(), err)
				doRemove = true
			}
		}
		if doRemove {
			cm.store.Clear(si.ID())
			cm.LeaseManager.Delete(ctx, leases.Lease{ID: si.ID()})
		}
	}
	return nil
}

// IdentityMapping returns the userns remapping used for refs
func (cm *cacheManager) IdentityMapping() *idtools.IdentityMapping {
	return cm.Snapshotter.IdentityMapping()
}

// Close closes the manager and releases the metadata database lock. No other
// method should be called after Close.
func (cm *cacheManager) Close() error {
	// TODO: allocate internal context and cancel it here
	return cm.store.Close()
}

// Get returns an immutable snapshot reference for ID
func (cm *cacheManager) Get(ctx context.Context, id string, opts ...RefOption) (ImmutableRef, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.get(ctx, id, opts...)
}

// get requires manager lock to be taken
func (cm *cacheManager) get(ctx context.Context, id string, opts ...RefOption) (*immutableRef, error) {
	rec, err := cm.getRecord(ctx, id, opts...)
	if err != nil {
		return nil, err
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()

	triggerUpdate := true
	for _, o := range opts {
		if o == NoUpdateLastUsed {
			triggerUpdate = false
		}
	}

	descHandlers := descHandlersOf(opts...)
	return rec.ref(triggerUpdate, descHandlers)
}

// getRecord returns record for id. Requires manager lock.
func (cm *cacheManager) getRecord(ctx context.Context, id string, opts ...RefOption) (cr *cacheRecord, retErr error) {
	checkLazyProviders := func(rec *cacheRecord) error {
		missing := NeedsRemoteProvidersError(nil)
		dhs := descHandlersOf(opts...)
		if err := rec.walkUniqueRecords(func(cr *cacheRecord) error {
			blob := cr.getBlob()
			if isLazy, err := cr.isLazy(ctx); err != nil {
				return err
			} else if isLazy && dhs[blob] == nil {
				missing = append(missing, blob)
			}
			return nil
		}); err != nil {
			return err
		}
		if len(missing) > 0 {
			return missing
		}
		return nil
	}

	if rec, ok := cm.records[id]; ok {
		if rec.dead {
			return nil, errors.Wrapf(errNotFound, "failed to get dead record %s", id)
		}
		if err := checkLazyProviders(rec); err != nil {
			return nil, err
		}
		return rec, nil
	}

	md, ok := cm.getMetadata(id)
	if !ok {
		return nil, errors.Wrap(errNotFound, id)
	}

	parents, err := cm.parentsOf(ctx, md, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get parents")
	}
	defer func() {
		if retErr != nil {
			parents.release(context.TODO())
		}
	}()

	rec := &cacheRecord{
		cm:            cm,
		cacheMetadata: md,
		parentRefs:    parents,
		immutableRefs: make(map[*immutableRef]struct{}),
	}

	// the record was deleted but we crashed before data on disk was removed
	if md.getDeleted() {
		if err := rec.remove(ctx); err != nil {
			return nil, err
		}
		return nil, errors.Wrapf(errNotFound, "failed to get deleted record %s", id)
	}

	if err := initializeMetadata(rec.cacheMetadata, rec.parentRefs, opts...); err != nil {
		return nil, err
	}

	if err := setImageRefMetadata(rec.cacheMetadata, opts...); err != nil {
		return nil, errors.Wrapf(err, "failed to append image ref metadata to ref %s", rec.ID())
	}

	switch rec.parentKind() {
	case Merge:
		rec.isFinalized = true
		for _, p := range rec.parentRefs.mergeParents {
			rec.isFinalized = p.isFinalized
			if !rec.isFinalized {
				break
			}
		}
	case Layer, None:
		rec.isFinalized = rec.getBlobOnly()
		if !rec.isFinalized {
			if info, err := rec.cm.Snapshotter.Stat(ctx, rec.getSnapshotID()); err != nil {
				return nil, errors.Wrapf(err, "failed to stat snapshot for ref %s", rec.ID())
			} else {
				rec.isFinalized = info.Kind == snapshots.KindCommitted
			}
		}
	}

	cm.records[id] = rec
	if err := checkLazyProviders(rec); err != nil {
		return nil, err
	}
	return rec, nil
}

func (cm *cacheManager) parentsOf(ctx context.Context, md *cacheMetadata, opts ...RefOption) (ps parentRefs, rerr error) {
	if parentID := md.getParent(); parentID != "" {
		if p, err := cm.get(ctx, parentID, append(opts, NoUpdateLastUsed)); err != nil {
			return ps, err
		} else {
			ps.layerParent = p
			return ps, nil
		}
	}
	for _, parentID := range md.getMergeParents() {
		if p, err := cm.get(ctx, parentID, append(opts, NoUpdateLastUsed)); err != nil {
			return ps, err
		} else {
			ps.mergeParents = append(ps.mergeParents, p)
		}
	}
	return ps, nil
}

func (cm *cacheManager) New(ctx context.Context, s ImmutableRef, sess session.Group, opts ...RefOption) (mr MutableRef, err error) {
	id := identity.NewID()

	var parent *immutableRef
	var parentSnapshotID string
	if s != nil {
		if _, ok := s.(*immutableRef); ok {
			parent = s.Clone().(*immutableRef)
		} else {
			p, err := cm.Get(ctx, s.ID(), append(opts, NoUpdateLastUsed)...)
			if err != nil {
				return nil, err
			}
			parent = p.(*immutableRef)
		}
		if err := parent.finalizeLocked(ctx); err != nil {
			return nil, err
		}
		if err := parent.PrepareMount(ctx, parent.descHandlers, sess); err != nil {
			return nil, err
		}
		parentSnapshotID = parent.getSnapshotID()
	}

	defer func() {
		if err != nil && parent != nil {
			parent.Release(context.TODO())
		}
	}()

	l, err := cm.ManagerOpt.LeaseManager.Create(ctx, func(l *leases.Lease) error {
		l.ID = id
		l.Labels = map[string]string{
			"containerd.io/gc.flat": time.Now().UTC().Format(time.RFC3339Nano),
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create lease")
	}

	defer func() {
		if err != nil {
			if err := cm.ManagerOpt.LeaseManager.Delete(context.TODO(), leases.Lease{
				ID: l.ID,
			}); err != nil {
				logrus.Errorf("failed to remove lease: %+v", err)
			}
		}
	}()

	snapshotID := identity.NewID()
	if err := cm.Snapshotter.AddLease(ctx, l.ID, snapshotID); err != nil {
		return nil, errors.Wrapf(err, "failed to add snapshot %s to lease", snapshotID)
	}

	if cm.Snapshotter.Name() == "stargz" && parent != nil {
		if rerr := parent.withRemoteSnapshotLabelsStargzMode(ctx, parent.descHandlers, sess, func() {
			err = cm.Snapshotter.Prepare(ctx, snapshotID, parentSnapshotID)
		}); rerr != nil {
			return nil, rerr
		}
	} else {
		err = cm.Snapshotter.Prepare(ctx, snapshotID, parentSnapshotID)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "failed to prepare %s", parentSnapshotID)
	}

	md, _ := cm.getMetadata(id)

	rec := &cacheRecord{
		cm:            cm,
		cacheMetadata: md,
		parentRefs:    parentRefs{layerParent: parent},
		immutableRefs: make(map[*immutableRef]struct{}),
	}

	opts = append(opts, withSnapshotID(snapshotID))
	if err := initializeMetadata(rec.cacheMetadata, rec.parentRefs, opts...); err != nil {
		return nil, err
	}

	if err := setImageRefMetadata(rec.cacheMetadata, opts...); err != nil {
		return nil, errors.Wrapf(err, "failed to append image ref metadata to ref %s", rec.ID())
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.records[id] = rec // TODO: save to db

	// parent refs are possibly lazy so keep it hold the description handlers.
	var dhs DescHandlers
	if parent != nil {
		dhs = parent.descHandlers
	}
	return rec.mref(true, dhs)
}

func (cm *cacheManager) GetMutable(ctx context.Context, id string, opts ...RefOption) (_ MutableRef, rerr error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	rec, err := cm.getRecord(ctx, id, opts...)
	if err != nil {
		return nil, err
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()

	mref, err := rec.mref(true, descHandlersOf(opts...))
	if err != nil {
		return nil, err
	}

	if !mref.getCommitted() {
		return mref, nil
	}
	// rec was committed but not finalized, so invalidate it by its ID and return a copy with the same metadata but new ID.

	newID := identity.NewID()

	md, _ := cm.getMetadata(newID)
	if err := mref.copyValuesTo(md); err != nil {
		return nil, err
	}
	snapshotID := md.getSnapshotID()

	l, err := cm.ManagerOpt.LeaseManager.Create(ctx, func(l *leases.Lease) error {
		l.ID = newID
		l.Labels = map[string]string{
			"containerd.io/gc.flat": time.Now().UTC().Format(time.RFC3339Nano),
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create lease")
	}
	defer func() {
		if rerr != nil {
			if err := cm.ManagerOpt.LeaseManager.Delete(context.TODO(), leases.Lease{
				ID: l.ID,
			}); err != nil {
				logrus.Errorf("failed to remove lease: %+v", err)
			}
		}
	}()

	if err := cm.Snapshotter.AddLease(ctx, l.ID, snapshotID); err != nil {
		return nil, errors.Wrapf(err, "failed to add snapshot %s to lease", snapshotID)
	}

	newRec := &cacheRecord{
		cm:            cm,
		cacheMetadata: md,
		parentRefs:    rec.parentRefs.clone(),
		immutableRefs: make(map[*immutableRef]struct{}),
	}
	cm.records[newID] = newRec

	newMref, err := newRec.mref(true, descHandlersOf(opts...))
	if err != nil {
		return nil, err
	}
	defer func() {
		if rerr != nil {
			newMref.Release(context.TODO())
		}
	}()

	if err := mref.SetCachePolicyDefault(); err != nil {
		return nil, err
	}
	if err := mref.release(ctx); err != nil {
		return nil, err
	}

	return newMref, nil
}

func (cm *cacheManager) Merge(ctx context.Context, inputParents []ImmutableRef, opts ...RefOption) (ir ImmutableRef, rerr error) {
	id := identity.NewID()

	var parents parentRefs
	var parentSnapshotIDs []string
	defer func() {
		if rerr != nil {
			parents.release(context.TODO())
		}
	}()
	for _, inputParent := range inputParents {
		var parent *immutableRef
		if _, ok := inputParent.(*immutableRef); ok {
			parent = inputParent.Clone().(*immutableRef)
		} else {
			if p, err := cm.Get(ctx, inputParent.ID(), append(opts, NoUpdateLastUsed)...); err != nil {
				return nil, err
			} else {
				parent = p.(*immutableRef)
			}
		}
		switch parent.parentKind() {
		case Merge:
			// if parent is itself a merge, flatten it out by just setting our parents directly to its parents
			for _, grandparent := range parent.mergeParents {
				parents.mergeParents = append(parents.mergeParents, grandparent.clone(false))
				parentSnapshotIDs = append(parentSnapshotIDs, grandparent.getSnapshotID())
			}
		case Layer, None:
			parents.mergeParents = append(parents.mergeParents, parent.clone(false))
			parentSnapshotIDs = append(parentSnapshotIDs, parent.getSnapshotID())
		}
	}

	// De-dupe parents; if a parent appears more than once, then remove all occurrences besides the last one.
	var filteredParents []*immutableRef
	memo := map[string]int{}
	for _, parent := range parents.mergeParents {
		if i, ok := memo[parent.getSnapshotID()]; ok {
			filteredParents[i] = nil
		}
		memo[parent.getSnapshotID()] = len(filteredParents)
		filteredParents = append(filteredParents, parent)
	}
	parents.mergeParents = nil
	for _, parent := range filteredParents {
		if parent != nil {
			parents.mergeParents = append(parents.mergeParents, parent)
		}
	}

	mergeDigest := parents.mergeDigest()

	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Check if there's already an equal merged ref, return it if so
	if mds, err := cm.searchMergeDigest(ctx, mergeDigest); err != nil {
		return nil, err
	} else {
		for _, md := range mds {
			if ref, err := cm.get(ctx, md.ID(), opts...); err != nil {
				bklog.G(ctx).Debugf("failed to get ref %q for matching merge digest %q", md.ID(), mergeDigest)
			} else {
				return ref, nil
			}
		}
	}

	// Build the new ref
	md, _ := cm.getMetadata(id)

	rec := &cacheRecord{
		cm:            cm,
		cacheMetadata: md,
		parentRefs:    parents,
		immutableRefs: make(map[*immutableRef]struct{}),
		isFinalized:   parents.areFinalized(),
	}

	if err := initializeMetadata(rec.cacheMetadata, rec.parentRefs, opts...); err != nil {
		return nil, err
	}

	snapshotID := id

	l, err := cm.LeaseManager.Create(ctx, func(l *leases.Lease) error {
		l.ID = id
		l.Labels = map[string]string{
			"containerd.io/gc.flat": time.Now().UTC().Format(time.RFC3339Nano),
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create lease")
	}
	defer func() {
		if rerr != nil {
			if err := cm.LeaseManager.Delete(context.TODO(), leases.Lease{
				ID: l.ID,
			}); err != nil {
				bklog.G(ctx).Errorf("failed to remove lease: %+v", err)
			}
		}
	}()

	if err := cm.Snapshotter.AddLease(ctx, l.ID, snapshotID); err != nil {
		return nil, errors.Wrapf(err, "failed to add snapshot %s to lease", id)
	}

	rec.queueMergeDigest(mergeDigest)
	rec.queueSnapshotID(snapshotID)
	if err := rec.commitMetadata(); err != nil {
		return nil, err
	}

	cm.records[id] = rec

	return rec.ref(true, descHandlersOf(opts...))
}

func (cm *cacheManager) Prune(ctx context.Context, ch chan client.UsageInfo, opts ...client.PruneInfo) error {
	cm.muPrune.Lock()

	for _, opt := range opts {
		if err := cm.pruneOnce(ctx, ch, opt); err != nil {
			cm.muPrune.Unlock()
			return err
		}
	}

	cm.muPrune.Unlock()

	if cm.GarbageCollect != nil {
		if _, err := cm.GarbageCollect(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (cm *cacheManager) pruneOnce(ctx context.Context, ch chan client.UsageInfo, opt client.PruneInfo) error {
	filter, err := filters.ParseAll(opt.Filter...)
	if err != nil {
		return errors.Wrapf(err, "failed to parse prune filters %v", opt.Filter)
	}

	var check ExternalRefChecker
	if f := cm.PruneRefChecker; f != nil && (!opt.All || len(opt.Filter) > 0) {
		c, err := f()
		if err != nil {
			return errors.WithStack(err)
		}
		check = c
	}

	totalSize := int64(0)
	if opt.KeepBytes != 0 {
		du, err := cm.DiskUsage(ctx, client.DiskUsageInfo{})
		if err != nil {
			return err
		}
		for _, ui := range du {
			if ui.Shared {
				continue
			}
			totalSize += ui.Size
		}
	}

	return cm.prune(ctx, ch, pruneOpt{
		filter:       filter,
		all:          opt.All,
		checkShared:  check,
		keepDuration: opt.KeepDuration,
		keepBytes:    opt.KeepBytes,
		totalSize:    totalSize,
	})
}

func (cm *cacheManager) prune(ctx context.Context, ch chan client.UsageInfo, opt pruneOpt) error {
	var toDelete []*deleteRecord

	if opt.keepBytes != 0 && opt.totalSize < opt.keepBytes {
		return nil
	}

	cm.mu.Lock()

	gcMode := opt.keepBytes != 0
	cutOff := time.Now().Add(-opt.keepDuration)

	locked := map[*cacheRecord]struct{}{}

	for _, cr := range cm.records {
		if _, ok := locked[cr]; ok {
			continue
		}
		cr.mu.Lock()

		if cr.dead {
			cr.mu.Unlock()
			continue
		}

		if cr.refCount() == 0 {
			recordType := cr.GetRecordType()
			if recordType == "" {
				recordType = client.UsageRecordTypeRegular
			}

			shared := false
			if opt.checkShared != nil {
				shared = opt.checkShared.Exists(cr.ID(), cr.layerDigestChain())
			}

			if !opt.all {
				if recordType == client.UsageRecordTypeInternal || recordType == client.UsageRecordTypeFrontend || shared {
					cr.mu.Unlock()
					continue
				}
			}

			c := &client.UsageInfo{
				ID:         cr.ID(),
				Mutable:    !cr.isFinalized,
				RecordType: recordType,
				Shared:     shared,
			}

			usageCount, lastUsedAt := cr.getLastUsed()
			c.LastUsedAt = lastUsedAt
			c.UsageCount = usageCount

			if opt.keepDuration != 0 {
				if lastUsedAt != nil && lastUsedAt.After(cutOff) {
					cr.mu.Unlock()
					continue
				}
			}

			if opt.filter.Match(adaptUsageInfo(c)) {
				toDelete = append(toDelete, &deleteRecord{
					cacheRecord: cr,
					lastUsedAt:  c.LastUsedAt,
					usageCount:  c.UsageCount,
				})
				if !gcMode {
					cr.dead = true

					// mark metadata as deleted in case we crash before cleanup finished
					if err := cr.queueDeleted(); err != nil {
						cr.mu.Unlock()
						cm.mu.Unlock()
						return err
					}
					if err := cr.commitMetadata(); err != nil {
						cr.mu.Unlock()
						cm.mu.Unlock()
						return err
					}
				} else {
					locked[cr] = struct{}{}
					continue // leave the record locked
				}
			}
		}
		cr.mu.Unlock()
	}

	if gcMode && len(toDelete) > 0 {
		sortDeleteRecords(toDelete)
		var err error
		for i, cr := range toDelete {
			// only remove single record at a time
			if i == 0 {
				cr.dead = true
				err = cr.queueDeleted()
				if err == nil {
					err = cr.commitMetadata()
				}
			}
			cr.mu.Unlock()
		}
		if err != nil {
			return err
		}
		toDelete = toDelete[:1]
	}

	cm.mu.Unlock()

	if len(toDelete) == 0 {
		return nil
	}

	// calculate sizes here so that lock does not need to be held for slow process
	for _, cr := range toDelete {
		if cr.getSize() == sizeUnknown {
			// calling size will warm cache for next call
			if _, err := cr.size(ctx); err != nil {
				return err
			}
		}
	}

	cm.mu.Lock()
	var err error
	for _, cr := range toDelete {
		cr.mu.Lock()

		usageCount, lastUsedAt := cr.getLastUsed()

		c := client.UsageInfo{
			ID:          cr.ID(),
			Mutable:     !cr.isFinalized,
			InUse:       cr.refCount() > 0,
			Size:        cr.getSize(),
			CreatedAt:   cr.GetCreatedAt(),
			Description: cr.GetDescription(),
			LastUsedAt:  lastUsedAt,
			UsageCount:  usageCount,
		}

		switch cr.parentKind() {
		case Layer:
			c.Parents = append(c.Parents, cr.layerParent.ID())
		case Merge:
			for _, p := range cr.mergeParents {
				c.Parents = append(c.Parents, p.ID())
			}
		}

		opt.totalSize -= c.Size

		if err1 := cr.remove(ctx); err == nil {
			err = err1
		}

		if err == nil && ch != nil {
			ch <- c
		}
		cr.mu.Unlock()
	}
	cm.mu.Unlock()
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return cm.prune(ctx, ch, opt)
	}
}

func (cm *cacheManager) markShared(m map[string]*cacheUsageInfo) error {
	if cm.PruneRefChecker == nil {
		return nil
	}
	c, err := cm.PruneRefChecker()
	if err != nil {
		return errors.WithStack(err)
	}

	var markAllParentsShared func(...string)
	markAllParentsShared = func(ids ...string) {
		for _, id := range ids {
			if v, ok := m[id]; ok {
				v.shared = true
				markAllParentsShared(v.parents...)
			}
		}
	}

	for id := range m {
		if m[id].shared {
			continue
		}
		if b := c.Exists(id, m[id].parentChain); b {
			markAllParentsShared(id)
		}
	}
	return nil
}

type cacheUsageInfo struct {
	refs        int
	parents     []string
	size        int64
	mutable     bool
	createdAt   time.Time
	usageCount  int
	lastUsedAt  *time.Time
	description string
	recordType  client.UsageRecordType
	shared      bool
	parentChain []digest.Digest
}

func (cm *cacheManager) DiskUsage(ctx context.Context, opt client.DiskUsageInfo) ([]*client.UsageInfo, error) {
	filter, err := filters.ParseAll(opt.Filter...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse diskusage filters %v", opt.Filter)
	}

	cm.mu.Lock()

	m := make(map[string]*cacheUsageInfo, len(cm.records))
	rescan := make(map[string]struct{}, len(cm.records))

	for id, cr := range cm.records {
		cr.mu.Lock()

		usageCount, lastUsedAt := cr.getLastUsed()
		c := &cacheUsageInfo{
			refs:        cr.refCount(),
			mutable:     !cr.isFinalized,
			size:        cr.getSize(),
			createdAt:   cr.GetCreatedAt(),
			usageCount:  usageCount,
			lastUsedAt:  lastUsedAt,
			description: cr.GetDescription(),
			recordType:  cr.GetRecordType(),
			parentChain: cr.layerDigestChain(),
		}
		if c.recordType == "" {
			c.recordType = client.UsageRecordTypeRegular
		}

		switch cr.parentKind() {
		case Layer:
			c.parents = append(c.parents, cr.layerParent.ID())
		case Merge:
			for _, p := range cr.mergeParents {
				c.parents = append(c.parents, p.ID())
			}
		}

		if cr.mutableRef != nil {
			c.size = 0 // size can not be determined because it is changing
		}
		m[id] = c
		rescan[id] = struct{}{}
		cr.mu.Unlock()
	}
	cm.mu.Unlock()

	for {
		if len(rescan) == 0 {
			break
		}
		for id := range rescan {
			v := m[id]
			if v.refs == 0 {
				for _, p := range v.parents {
					m[p].refs--
					rescan[p] = struct{}{}
				}
			}
			delete(rescan, id)
		}
	}

	if err := cm.markShared(m); err != nil {
		return nil, err
	}

	var du []*client.UsageInfo
	for id, cr := range m {
		c := &client.UsageInfo{
			ID:          id,
			Mutable:     cr.mutable,
			InUse:       cr.refs > 0,
			Size:        cr.size,
			Parents:     cr.parents,
			CreatedAt:   cr.createdAt,
			Description: cr.description,
			LastUsedAt:  cr.lastUsedAt,
			UsageCount:  cr.usageCount,
			RecordType:  cr.recordType,
			Shared:      cr.shared,
		}
		if filter.Match(adaptUsageInfo(c)) {
			du = append(du, c)
		}
	}

	eg, ctx := errgroup.WithContext(ctx)

	for _, d := range du {
		if d.Size == sizeUnknown {
			func(d *client.UsageInfo) {
				eg.Go(func() error {
					cm.mu.Lock()
					ref, err := cm.get(ctx, d.ID, NoUpdateLastUsed)
					cm.mu.Unlock()
					if err != nil {
						d.Size = 0
						return nil
					}
					s, err := ref.size(ctx)
					if err != nil {
						return err
					}
					d.Size = s
					return ref.Release(context.TODO())
				})
			}(d)
		}
	}

	if err := eg.Wait(); err != nil {
		return du, err
	}

	return du, nil
}

func IsNotFound(err error) bool {
	return errors.Is(err, errNotFound)
}

type RefOption interface{}

type cachePolicy int

const (
	cachePolicyDefault cachePolicy = iota
	cachePolicyRetain
)

type noUpdateLastUsed struct{}

var NoUpdateLastUsed noUpdateLastUsed

func CachePolicyRetain(m *cacheMetadata) error {
	return m.SetCachePolicyRetain()
}

func CachePolicyDefault(m *cacheMetadata) error {
	return m.SetCachePolicyDefault()
}

func WithDescription(descr string) RefOption {
	return func(m *cacheMetadata) error {
		return m.queueDescription(descr)
	}
}

func WithRecordType(t client.UsageRecordType) RefOption {
	return func(m *cacheMetadata) error {
		return m.queueRecordType(t)
	}
}

func WithCreationTime(tm time.Time) RefOption {
	return func(m *cacheMetadata) error {
		return m.queueCreatedAt(tm)
	}
}

func withSnapshotID(id string) RefOption {
	return func(m *cacheMetadata) error {
		return m.queueSnapshotID(id)
	}
}

// Need a separate type for imageRef because it needs to be called outside
// initializeMetadata while still being a RefOption, so wrapping it in a
// different type ensures initializeMetadata won't catch it too and duplicate
// setting the metadata.
type imageRefOption func(m *cacheMetadata) error

// WithImageRef appends the given imageRef to the cache ref's metadata
func WithImageRef(imageRef string) RefOption {
	return imageRefOption(func(m *cacheMetadata) error {
		return m.appendImageRef(imageRef)
	})
}

func setImageRefMetadata(m *cacheMetadata, opts ...RefOption) error {
	for _, opt := range opts {
		if fn, ok := opt.(imageRefOption); ok {
			if err := fn(m); err != nil {
				return err
			}
		}
	}
	return m.commitMetadata()
}

func initializeMetadata(m *cacheMetadata, parents parentRefs, opts ...RefOption) error {
	if tm := m.GetCreatedAt(); !tm.IsZero() {
		return nil
	}

	switch parents.parentKind() {
	case Merge:
		var ids []string
		for _, p := range parents.mergeParents {
			ids = append(ids, p.ID())
		}
		if err := m.queueMergeParents(ids); err != nil {
			return err
		}
	case Layer:
		if parents.layerParent != nil {
			if err := m.queueParent(parents.layerParent.ID()); err != nil {
				return err
			}
		}
	}

	if err := m.queueCreatedAt(time.Now()); err != nil {
		return err
	}

	for _, opt := range opts {
		if fn, ok := opt.(func(*cacheMetadata) error); ok {
			if err := fn(m); err != nil {
				return err
			}
		}
	}

	return m.commitMetadata()
}

func adaptUsageInfo(info *client.UsageInfo) filters.Adaptor {
	return filters.AdapterFunc(func(fieldpath []string) (string, bool) {
		if len(fieldpath) == 0 {
			return "", false
		}

		switch fieldpath[0] {
		case "id":
			return info.ID, info.ID != ""
		case "parent":
			return strings.Join(info.Parents, ","), len(info.Parents) > 0
		case "description":
			return info.Description, info.Description != ""
		case "inuse":
			return "", info.InUse
		case "mutable":
			return "", info.Mutable
		case "immutable":
			return "", !info.Mutable
		case "type":
			return string(info.RecordType), info.RecordType != ""
		case "shared":
			return "", info.Shared
		case "private":
			return "", !info.Shared
		}

		// TODO: add int/datetime/bytes support for more fields

		return "", false
	})
}

type pruneOpt struct {
	filter       filters.Filter
	all          bool
	checkShared  ExternalRefChecker
	keepDuration time.Duration
	keepBytes    int64
	totalSize    int64
}

type deleteRecord struct {
	*cacheRecord
	lastUsedAt      *time.Time
	usageCount      int
	lastUsedAtIndex int
	usageCountIndex int
}

func sortDeleteRecords(toDelete []*deleteRecord) {
	sort.Slice(toDelete, func(i, j int) bool {
		if toDelete[i].lastUsedAt == nil {
			return true
		}
		if toDelete[j].lastUsedAt == nil {
			return false
		}
		return toDelete[i].lastUsedAt.Before(*toDelete[j].lastUsedAt)
	})

	maxLastUsedIndex := 0
	var val time.Time
	for _, v := range toDelete {
		if v.lastUsedAt != nil && v.lastUsedAt.After(val) {
			val = *v.lastUsedAt
			maxLastUsedIndex++
		}
		v.lastUsedAtIndex = maxLastUsedIndex
	}

	sort.Slice(toDelete, func(i, j int) bool {
		return toDelete[i].usageCount < toDelete[j].usageCount
	})

	maxUsageCountIndex := 0
	var count int
	for _, v := range toDelete {
		if v.usageCount != count {
			count = v.usageCount
			maxUsageCountIndex++
		}
		v.usageCountIndex = maxUsageCountIndex
	}

	sort.Slice(toDelete, func(i, j int) bool {
		return float64(toDelete[i].lastUsedAtIndex)/float64(maxLastUsedIndex)+
			float64(toDelete[i].usageCountIndex)/float64(maxUsageCountIndex) <
			float64(toDelete[j].lastUsedAtIndex)/float64(maxLastUsedIndex)+
				float64(toDelete[j].usageCountIndex)/float64(maxUsageCountIndex)
	})
}

func diffIDFromDescriptor(desc ocispec.Descriptor) (digest.Digest, error) {
	diffIDStr, ok := desc.Annotations["containerd.io/uncompressed"]
	if !ok {
		return "", errors.Errorf("missing uncompressed annotation for %s", desc.Digest)
	}
	diffID, err := digest.Parse(diffIDStr)
	if err != nil {
		return "", errors.Wrapf(err, "failed to parse diffID %q for %s", diffIDStr, desc.Digest)
	}
	return diffID, nil
}
