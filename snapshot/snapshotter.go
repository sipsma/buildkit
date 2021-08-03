package snapshot

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/diff"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/docker/docker/pkg/idtools"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/leaseutil"
	"github.com/pkg/errors"
)

const mergeParentsLabelKey = "containerd.io/snapshot/buildkit.mergeParents"

const securityCapabilityXattr = "security.capability"

type Mountable interface {
	Mount() ([]mount.Mount, func() error, error)
	IdentityMapping() *idtools.IdentityMapping
}

// Snapshotter defines interface that any snapshot implementation should satisfy
type Snapshotter interface {
	Name() string
	// Mounts returns the mounts for the given key. Unlike the containerd snapshotter interface, if the provided key is
	// in a committed state, read-only mounts will be returned for it (rather than forcing the caller to create a view
	// and get the mounts for that).
	Mounts(ctx context.Context, key string) (Mountable, error)
	Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) error
	Stat(ctx context.Context, key string) (snapshots.Info, error)
	Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error)
	Usage(ctx context.Context, key string) (snapshots.Usage, error)
	// Commit turns the snapshot (pointed to by the provided key) from active to committed. Unlike the containerd
	// snapshotter interface, this interface does not require you to change key.
	Commit(ctx context.Context, key string, opts ...snapshots.Opt) error
	Walk(ctx context.Context, fn snapshots.WalkFunc, filters ...string) error
	Close() error
	IdentityMapping() *idtools.IdentityMapping
	// Containerd returns the underlying containerd snapshotter interface used by this snapshotter
	Containerd() snapshots.Snapshotter
	AddLease(ctx context.Context, leaseID string, key string) error
	RemoveLease(ctx context.Context, leaseID string, key string) error
	Merge(ctx context.Context, key string, parents []string, opts ...snapshots.Opt) error
}

var overlayMergeSnapshotters = []string{
	"overlayfs",
	"stargz",
}

var hardlinkMergeSnapshotters = []string{
	"native",
}

func FromContainerdSnapshotter(
	ctx context.Context,
	name string,
	s snapshots.Snapshotter,
	idmap *idtools.IdentityMapping,
	leaseManager leases.Manager,
	applier diff.Applier,
	differ diff.Comparer,
) Snapshotter {
	var useOverlay bool
	for _, sn := range overlayMergeSnapshotters {
		if name == sn {
			useOverlay = true
			break
		}
	}
	var userxattr bool
	if useOverlay {
		var err error
		userxattr, err = needsUserXAttr(ctx, s)
		if err != nil {
			bklog.G(ctx).Errorf("failed to check userxattr, defaulting to %t: %v", userxattr, err)
		}
	}

	var useHardlinks bool
	for _, sn := range hardlinkMergeSnapshotters {
		if name == sn {
			useHardlinks = true
			break
		}
	}

	return &fromContainerd{
		name:         name,
		snapshotter:  s,
		leaseManager: leaseManager,
		idmap:        idmap,
		useOverlay:   useOverlay,
		userxattr:    userxattr,
		useHardlinks: useHardlinks,
		applier:      applier,
		differ:       differ,
	}
}

type fromContainerd struct {
	mu           sync.Mutex
	name         string
	snapshotter  snapshots.Snapshotter
	leaseManager leases.Manager
	idmap        *idtools.IdentityMapping
	useOverlay   bool
	userxattr    bool
	useHardlinks bool
	applier      diff.Applier
	differ       diff.Comparer
}

func (sn *fromContainerd) activeKey(key string) string {
	if key == "" {
		return ""
	}
	split := strings.Split(key, "-")
	return split[0] + "-active"
}

func (sn *fromContainerd) commitKey(key string) string {
	if key == "" {
		return ""
	}
	split := strings.Split(key, "-")
	return split[0] + "-commit"
}

func (sn *fromContainerd) viewKey(key string) string {
	if key == "" {
		return ""
	}
	split := strings.Split(key, "-")
	return split[0] + "-view"
}

func (sn *fromContainerd) viewLeaseID(leaseID string) string {
	if leaseID == "" {
		return ""
	}
	split := strings.Split(leaseID, "-")
	return split[0] + "-view"
}

func (sn *fromContainerd) currentKey(ctx context.Context, key string) (string, error) {
	info, err := sn.Stat(ctx, key)
	if err != nil {
		return "", err
	}
	return info.Name, nil
}

func (sn *fromContainerd) setMergeParents(ctx context.Context, key string, parents []string) error {
	key, err := sn.currentKey(ctx, key)
	if err != nil {
		return err
	}

	if _, err := sn.snapshotter.Update(ctx, snapshots.Info{
		Name:   key,
		Labels: map[string]string{mergeParentsLabelKey: strings.Join(parents, ",")},
	}, "labels."+mergeParentsLabelKey); err != nil {
		return err
	}
	return nil
}

func (sn *fromContainerd) parentsOf(ctx context.Context, key string) ([]string, error) {
	if info, err := sn.Stat(ctx, key); err != nil {
		return nil, err
	} else if parentStr, ok := info.Labels[mergeParentsLabelKey]; ok {
		return strings.Split(parentStr, ","), nil
	} else if info.Parent != "" {
		return []string{info.Parent}, nil
	} else {
		return nil, nil
	}
}

func (sn *fromContainerd) Name() string {
	return sn.name
}

func (sn *fromContainerd) IdentityMapping() *idtools.IdentityMapping {
	return sn.idmap
}

func (sn *fromContainerd) Close() error {
	return sn.snapshotter.Close()
}

func (sn *fromContainerd) Containerd() snapshots.Snapshotter {
	return sn.snapshotter
}

func (sn *fromContainerd) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	// Figure out which snapshot ID is currently underlying the provided key, which
	// depends on whether the snapshot has been committed or is still active. Also check
	// "key" by itself in order to be compatible with buildkit installations that have
	// been upgraded from older versions that didn't use the same snapshot ID naming scheme.
	for _, key := range []string{sn.commitKey(key), sn.activeKey(key), key} {
		if info, err := sn.snapshotter.Stat(ctx, key); err == nil {
			return info, nil
		} else if !errdefs.IsNotFound(err) {
			return snapshots.Info{}, err
		}
	}

	return snapshots.Info{}, errors.Wrapf(errdefs.ErrNotFound, key)
}

func (sn *fromContainerd) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	key, err := sn.currentKey(ctx, key)
	if err != nil {
		return snapshots.Usage{}, err
	}
	return sn.snapshotter.Usage(ctx, key)
}

func (sn *fromContainerd) Mounts(ctx context.Context, key string) (_ Mountable, rerr error) {
	if !sn.useOverlay {
		// in the inefficient case, underlying snapshot always has the full merged contents, so just return its mounts
		mounts, err := sn.mounts(ctx, key)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get mounts")
		}
		return &staticMountable{mounts: mounts, idmap: sn.idmap, id: key}, nil
	}

	// in the efficient case, underlying snapshot may just be one in a series to be merged
	merged := mergedOverlay{idmap: sn.idmap, userxattr: sn.userxattr}

	stack := [][]string{{key}}
	var depsFinished bool
	for len(stack) > 0 {
		snaps := stack[len(stack)-1]
		if len(snaps) == 0 {
			stack = stack[:len(stack)-1]
			depsFinished = true
			continue
		}

		snap := snaps[0]
		parents, err := sn.parentsOf(ctx, snap)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get parents of %s", snap)
		}

		if !depsFinished && len(parents) > 0 {
			stack = append(stack, parents)
			continue
		}
		stack[len(stack)-1] = snaps[1:]
		depsFinished = false

		if len(parents) > 1 {
			// this is a merge snapshot, append the snapshot parents to the merged mount
			for _, parent := range parents {
				mounts, err := sn.mounts(ctx, parent)
				if err != nil {
					return nil, errors.Wrapf(err, "failed to get mounts of parent %s", snap)
				}
				merged.parents = append(merged.parents, mounts...)
			}
		} else if snap == key {
			// this isn't a merge snapshot, but it's the key whose mounts are being requested, so it's included
			mounts, err := sn.mounts(ctx, snap)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to get mounts of %s", snap)
			}
			merged.parents = append(merged.parents, mounts...)
		}
	}

	return merged, nil
}

func (sn *fromContainerd) mounts(ctx context.Context, key string) (_ []mount.Mount, rerr error) {
	info, err := sn.Stat(ctx, key)
	if err != nil {
		return nil, err
	}
	key = info.Name

	if info.Kind == snapshots.KindCommitted {
		// setup a view for the mounts
		existingLease, ok := leases.FromContext(ctx)
		if !ok {
			return nil, fmt.Errorf("no lease to add %s view to", key)
		}
		leaseID := sn.viewLeaseID(existingLease)

		sn.mu.Lock()
		if _, err := sn.leaseManager.Create(ctx, func(l *leases.Lease) error {
			l.ID = leaseID
			l.Labels = map[string]string{
				"containerd.io/gc.flat": time.Now().UTC().Format(time.RFC3339Nano),
			}
			return nil
		}, leaseutil.MakeTemporary); err != nil && !errdefs.IsAlreadyExists(err) {
			sn.mu.Unlock()
			return nil, err
		} else if err == nil {
			defer func() {
				if rerr != nil {
					sn.leaseManager.Delete(context.TODO(), leases.Lease{ID: leaseID})
				}
			}()
		}
		sn.mu.Unlock()

		viewID := sn.viewKey(key)
		if _, err := sn.snapshotter.View(
			leases.WithLease(ctx, leaseID),
			viewID,
			key,
		); err != nil && !errdefs.IsAlreadyExists(err) {
			return nil, errors.Wrapf(err, "failed to create view for %s", key)
		}
		key = viewID
	}

	return sn.snapshotter.Mounts(ctx, key)
}

func (sn *fromContainerd) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) error {
	origParent := parent
	var parentIsMerge bool
	if parent != "" {
		var err error
		parentInfo, err := sn.Stat(ctx, parent)
		if err != nil {
			return err
		}
		parent = parentInfo.Name
		parentIsMerge = len(strings.Split(parentInfo.Labels[mergeParentsLabelKey], ",")) > 1
	}

	if sn.useOverlay && parentIsMerge {
		parent = ""
	}

	_, err := sn.snapshotter.Prepare(ctx, sn.activeKey(key), parent, opts...)
	if err != nil {
		return errors.Wrapf(err, "failed to prepare %q as %q", parent, sn.activeKey(key))
	}

	if sn.useOverlay && parentIsMerge {
		if err := sn.setMergeParents(ctx, sn.activeKey(key), []string{origParent}); err != nil {
			return errors.Wrapf(err, "failed to set parents of newly prepared %q", sn.activeKey(key))
		}
	}
	return nil
}

func (sn *fromContainerd) Commit(ctx context.Context, key string, opts ...snapshots.Opt) error {
	info, err := sn.Stat(ctx, key)
	if err != nil {
		return err
	}
	opts = append(opts, snapshots.WithLabels(snapshots.FilterInheritedLabels(info.Labels)))
	return sn.snapshotter.Commit(ctx, sn.commitKey(key), info.Name, opts...)
}

func (sn *fromContainerd) AddLease(ctx context.Context, leaseID string, key string) error {
	for _, id := range []string{sn.activeKey(key), sn.commitKey(key), key} {
		if err := sn.leaseManager.AddResource(ctx, leases.Lease{ID: leaseID}, leases.Resource{
			ID:   id,
			Type: "snapshots/" + sn.Name(),
		}); err != nil && !errdefs.IsAlreadyExists(err) {
			return err
		}
	}

	viewLeaseID := sn.viewLeaseID(leaseID)
	if _, err := sn.leaseManager.Create(ctx, func(l *leases.Lease) error {
		l.ID = viewLeaseID
		l.Labels = map[string]string{
			"containerd.io/gc.flat": time.Now().UTC().Format(time.RFC3339Nano),
		}
		return nil
	}, leaseutil.MakeTemporary); err != nil && !errdefs.IsAlreadyExists(err) {
		return err
	}
	if err := sn.leaseManager.AddResource(ctx, leases.Lease{ID: viewLeaseID}, leases.Resource{
		ID:   sn.viewKey(key),
		Type: "snapshots/" + sn.Name(),
	}); err != nil && !errdefs.IsAlreadyExists(err) {
		return err
	}

	return nil
}

// TODO:(sipsma) You shouldn't be able to delete a snapshot that is the parent of another merge
// ^^ or you should be able to but that inherently deletes the child merge
func (sn *fromContainerd) RemoveLease(ctx context.Context, leaseID string, key string) error {
	for _, id := range []string{sn.activeKey(key), sn.commitKey(key), key} {
		if err := sn.leaseManager.DeleteResource(ctx, leases.Lease{ID: leaseID}, leases.Resource{
			ID:   id,
			Type: "snapshots/" + sn.Name(),
		}); err != nil && !errdefs.IsNotFound(err) {
			return err
		}
	}

	if err := sn.leaseManager.Delete(ctx, leases.Lease{ID: sn.viewLeaseID(leaseID)}); err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	return nil
}

func (sn *fromContainerd) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	info, err := sn.Stat(ctx, info.Name)
	if err != nil {
		return snapshots.Info{}, err
	}
	return sn.snapshotter.Update(ctx, info, fieldpaths...)
}

func (sn *fromContainerd) Walk(ctx context.Context, fn snapshots.WalkFunc, filters ...string) error {
	return sn.snapshotter.Walk(ctx, fn, filters...)
}

func (sn *fromContainerd) Merge(ctx context.Context, key string, parents []string, opts ...snapshots.Opt) error {
	var filteredParents []string
	for _, p := range parents {
		if p != "" {
			filteredParents = append(filteredParents, p)
		}
	}
	parents = filteredParents

	if len(parents) < 2 {
		return fmt.Errorf("invalid merge: there must be at least two non-empty parents")
	}

	if !sn.useOverlay {
		return sn.inefficientMerge(ctx, key, parents, opts...)
	}
	return sn.efficientMerge(ctx, key, parents, opts...)
}

func (sn *fromContainerd) inefficientMerge(ctx context.Context, key string, parents []string, opts ...snapshots.Opt) error {
	var applyChains [][]string

	// flatten parents into each key in their chain
	for _, parent := range parents {
		var chain []string
		for curkey := parent; curkey != ""; {
			info, err := sn.Stat(ctx, curkey)
			if err != nil {
				return errors.Wrapf(err, "failed to stat chain key %q", curkey)
			}
			chain = append(chain, info.Name)
			curkey = info.Parent
		}
		applyChains = append(applyChains, chain)
	}

	// remove duplicates so that only the uppermost occurrence of a snapshot is applied
	memo := make(map[string]struct{})
	var filteredChains [][]string
	for i := range applyChains {
		chain := applyChains[len(applyChains)-1-i]
		var filteredChain []string
		for _, key := range chain {
			if _, ok := memo[key]; ok {
				continue
			}
			if key != "" {
				memo[key] = struct{}{}
			}
			filteredChain = append(filteredChain, key)
		}
		if filteredChain != nil {
			filteredChain = append(filteredChain, "") // include empty layer as the base of each chain
			filteredChains = append(filteredChains, filteredChain)
		}
	}
	applyChains = filteredChains

	var parentID string
	// the first parent will be the top of the chain we apply the rest to
	if parentInfo, err := sn.Stat(ctx, applyChains[len(applyChains)-1][0]); err != nil {
		return err
	} else if parentInfo.Kind == snapshots.KindCommitted {
		parentID = parentInfo.Name
		applyChains = applyChains[:len(applyChains)-1]
	} else {
		// if parents[0] is still active, we can't use it as the start of our new chain, so use its parent instead
		parentID = parentInfo.Parent
		applyChains[len(applyChains)-1] = applyChains[len(applyChains)-1][:2]
	}

	if err := sn.Prepare(ctx, key, parentID); err != nil {
		return errors.Wrapf(err, "failed to prepare %q as %q", parentID, key)
	}
	applyMountable, err := sn.Mounts(ctx, key)
	if err != nil {
		return errors.Wrapf(err, "failed to get mounts of %q", key)
	}
	applyMounts, unmount, err := applyMountable.Mount()
	if err != nil {
		return errors.Wrapf(err, "failed to mount %q", key)
	}

	// iterate over (lower, upper) pairs in each applyChain, calculating their diffs and applying each one to the mount
	// the chains had to be constructed in reverse order (child->parent), so iterate in reverse
	for i := range applyChains {
		chain := applyChains[len(applyChains)-1-i]
		for j := range chain[:len(chain)-1] {
			lower := chain[len(chain)-1-j]
			upper := chain[len(chain)-2-j]

			var lowerMounts []mount.Mount
			if lower != "" {
				var err error
				lowerMounts, err = sn.mounts(ctx, lower)
				if err != nil {
					return errors.Wrapf(err, "failed to get mounts of lower %q", lower)
				}
			}

			upperMounts, err := sn.mounts(ctx, upper)
			if err != nil {
				return errors.Wrapf(err, "failed to get mounts of upper %q", upper)
			}

			if err := diffApplyMerge(ctx, lowerMounts, upperMounts, applyMounts, sn.useHardlinks); err != nil {
				return err
			}
		}
	}

	if err := unmount(); err != nil {
		return errors.Wrapf(err, "failed to unmount %q", key)
	}
	if err := sn.Commit(ctx, key); err != nil {
		return errors.Wrapf(err, "failed to commit %q", key)
	}
	if err := sn.setMergeParents(ctx, key, parents); err != nil {
		return errors.Wrapf(err, "failed to set merge parents to %v", parents)
	}
	return nil
}

func (sn *fromContainerd) efficientMerge(ctx context.Context, key string, parents []string, opts ...snapshots.Opt) error {
	var flattenedParents []string
	for _, parent := range parents {
		if grandparents, err := sn.parentsOf(ctx, parent); err != nil {
			return errors.Wrap(err, "failed to get parents during merge")
		} else if len(grandparents) > 1 {
			flattenedParents = append(flattenedParents, grandparents...)
		} else {
			flattenedParents = append(flattenedParents, parent)
		}
	}
	parents = flattenedParents
	if err := sn.Prepare(ctx, key, "", opts...); err != nil {
		return errors.Wrap(err, "failed to prepare new merge snapshot")
	}
	if err := sn.Commit(ctx, key, opts...); err != nil {
		return errors.Wrap(err, "failed to commit new merge snapshot")
	}
	if err := sn.setMergeParents(ctx, key, parents); err != nil {
		return errors.Wrapf(err, "failed to set merge parents to %v", parents)
	}
	return nil
}
