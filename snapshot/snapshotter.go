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
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/leaseutil"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

type Mountable interface {
	Mount() ([]mount.Mount, func() error, error)
	IdentityMapping() *idtools.IdentityMapping
}

// Snapshotter defines interface that any snapshot implementation should satisfy
type Snapshotter interface {
	Name() string
	IdentityMapping() *idtools.IdentityMapping
	Close() error
	Containerd() snapshots.Snapshotter
	Usage(ctx context.Context, key string) (snapshots.Usage, error)
	Stat(ctx context.Context, key string) (snapshots.Info, error)
	Mounts(ctx context.Context, key string) (Mountable, error)
	Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) error
	// TODO doc diff between this Commit and underlying snapshotter one (don't have to change keys)
	Commit(ctx context.Context, key string, opts ...snapshots.Opt) error
	Merge(ctx context.Context, key string, parents []string, opts ...snapshots.Opt) error
	AddLease(ctx context.Context, key string) error
	RemoveLease(ctx context.Context, key string) error
	RemoveViewLease(ctx context.Context, key string) error
	Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error)
	// TODO remove Walk if you can remove migrate_v2
	Walk(ctx context.Context, fn snapshots.WalkFunc, filters ...string) error
}

var efficientMergeSnapshotters = []string{
	"overlayfs",
	"stargz",
}

func FromContainerdSnapshotter(
	name string,
	s snapshots.Snapshotter,
	applier diff.Applier,
	differ diff.Comparer,
	leaseManager leases.Manager,
	idmap *idtools.IdentityMapping,
) Snapshotter {
	// TODO could also determine by creating test snapshots and seeing if they use expected mount type, but not really worth it
	var useOverlay bool
	for _, sn := range efficientMergeSnapshotters {
		if name == sn {
			useOverlay = true
			break
		}
	}

	return &fromContainerd{
		name:         name,
		snapshotter:  s,
		idmap:        idmap,
		useOverlay:   useOverlay,
		leaseManager: leaseManager,
		applier:      applier,
		differ:       differ,
	}
}

type fromContainerd struct {
	name  string
	idmap *idtools.IdentityMapping
	// TODO is mu really needed? double check
	mu sync.Mutex

	snapshotter  snapshots.Snapshotter
	useOverlay   bool
	leaseManager leases.Manager
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

func (sn *fromContainerd) viewOf(ctx context.Context, key string) (_ string, rerr error) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	existingLease, ok := leases.FromContext(ctx)
	if !ok {
		return "", fmt.Errorf("no lease to add %s view to", key)
	}
	leaseID := existingLease + "-view"

	if _, err := sn.leaseManager.Create(ctx, func(l *leases.Lease) error {
		l.ID = leaseID
		l.Labels = map[string]string{
			"containerd.io/gc.flat": time.Now().UTC().Format(time.RFC3339Nano),
		}
		return nil
	}, leaseutil.MakeTemporary); err != nil && !errdefs.IsAlreadyExists(err) {
		return "", err
	} else if err == nil {
		defer func() {
			if rerr != nil {
				sn.leaseManager.Delete(context.TODO(), leases.Lease{ID: leaseID})
			}
		}()
	}

	viewID := sn.viewKey(key)
	if _, err := sn.snapshotter.View(
		leases.WithLease(ctx, leaseID),
		viewID,
		sn.commitKey(key),
	); err != nil && !errdefs.IsAlreadyExists(err) {
		return "", errors.Wrapf(err, "failed to create view for %s", key)
	}
	return viewID, nil
}

func (sn *fromContainerd) mountsOf(ctx context.Context, key string) (_ []mount.Mount, rerr error) {
	info, err := sn.Stat(ctx, key)
	if err != nil {
		return nil, err
	}
	key = info.Name

	if info.Kind == snapshots.KindCommitted {
		viewID, err := sn.viewOf(ctx, key)
		if err != nil {
			return nil, err
		}
		key = viewID
	}

	return sn.snapshotter.Mounts(ctx, key)
}

func (sn *fromContainerd) setMergeParents(ctx context.Context, key string, parents []string) error {
	info, err := sn.Stat(ctx, key)
	if err != nil {
		return err
	}
	key = info.Name

	if _, err := sn.snapshotter.Update(ctx, snapshots.Info{
		Name:   key,
		Labels: map[string]string{"buildkit.mergeParents": strings.Join(parents, ",")}, // TODO check that "," doesn't appear in parent IDs
	}, "labels.buildkit.mergeParents"); err != nil {
		return err
	}
	return nil
}

func (sn *fromContainerd) parentsOf(ctx context.Context, key string) ([]string, error) {
	if info, err := sn.Stat(ctx, key); err != nil {
		return nil, err
	} else if parentStr, ok := info.Labels["buildkit.mergeParents"]; ok {
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

func (sn *fromContainerd) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	info, err := sn.Stat(ctx, key)
	if err != nil {
		return snapshots.Usage{}, err
	}
	return sn.snapshotter.Usage(ctx, info.Name)
}

func (sn *fromContainerd) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	split := strings.Split(key, "-")
	if len(split) > 1 {
		return sn.snapshotter.Stat(ctx, key)
	}
	for _, key := range []string{sn.commitKey(key), sn.activeKey(key)} {
		if info, err := sn.snapshotter.Stat(ctx, key); err == nil {
			return info, nil
		} else if !errdefs.IsNotFound(err) {
			return snapshots.Info{}, err
		}
	}

	return snapshots.Info{}, errors.Wrapf(errdefs.ErrNotFound, key)
}

func (sn *fromContainerd) Mounts(ctx context.Context, key string) (Mountable, error) {
	if !sn.useOverlay {
		// in the inefficient case, underlying snapshot always has the full merged contents, so just return its mounts
		mounts, err := sn.mountsOf(ctx, key)
		if err != nil {
			return nil, err
		}
		return &staticMountable{mounts: mounts, idmap: sn.idmap, id: key}, nil
	}

	// in the efficient case, underlying snapshot may just be one in a series to be merged
	merged := mergedMount{idmap: sn.idmap}

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
		depsFinished = false

		if len(parents) > 1 {
			// this a merge snapshot, append the snapshot parents to the merged mount
			for _, parent := range parents {
				mounts, err := sn.mountsOf(ctx, parent)
				if err != nil {
					return nil, errors.Wrapf(err, "failed to get mounts of parent %s", snap)
				}
				merged.parents = append(merged.parents, mounts...)
			}
		} else if snap == key {
			// this isn't a merge snapshot, but it's the key whose mounts are being requested, so it's included
			mounts, err := sn.mountsOf(ctx, snap)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to get mounts of %s", snap)
			}
			merged.parents = append(merged.parents, mounts...)
		}

		stack[len(stack)-1] = snaps[1:]
	}

	return merged, nil
}

func (sn *fromContainerd) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) error {
	_, err := sn.snapshotter.Prepare(ctx, sn.activeKey(key), sn.commitKey(parent), opts...)
	return err
}

func (sn *fromContainerd) Commit(ctx context.Context, key string, opts ...snapshots.Opt) error {
	// TODO copy mergeParents label (unless that happens automatically)
	return sn.snapshotter.Commit(ctx, sn.commitKey(key), sn.activeKey(key), opts...)
}

func (sn *fromContainerd) Merge(ctx context.Context, key string, parents []string, opts ...snapshots.Opt) error {
	if len(parents) < 2 {
		// TODO actual error type
		return fmt.Errorf("invalid merge: there must be at least two parents: %v", parents)
	}

	if !sn.useOverlay {
		var applyKeys []string

		// the first parent will be the start of the chain we apply the rest to
		var parentID string
		if info, err := sn.Stat(ctx, parents[0]); err != nil {
			return err
		} else if info.Kind == snapshots.KindCommitted {
			parentID = info.Name
		} else {
			// if parents[0] is still active, we can't use it as the start of our new chain, so use its parent instead
			parentID = info.Parent
			applyKeys = []string{"", parents[0]}
		}

		// flatten the rest of the parents into each key in their chains
		for _, parent := range parents[1:] {
			var subkeys []string
			subkey := parent
			for subkey != "" {
				info, err := sn.Stat(ctx, subkey)
				if err != nil {
					return errors.Wrapf(err, "failed to stat subkey %q", subkey)
				}
				subkeys = append(subkeys, info.Name)
				subkey = info.Parent
			}
			subkeys = append(subkeys, "") // include empty layer as the bottom of each chain
			// the subkeys of each parent were traversed in reverse order (child->parent), so reverse the order back here
			for i := range subkeys {
				applyKeys = append(applyKeys, subkeys[len(subkeys)-1-i])
			}
		}

		// iterate over (lower, upper) pairs in the applyKeys, calculating the diff and applying the to the mount
		for i, lower := range applyKeys[:len(applyKeys)-1] {
			// TODO handle clearing views when done (or on error). Also other cleanup crap
			upper := applyKeys[i+1]
			if upper == "" {
				// TODO this case is sort of ugly, maybe have a [][]string instead to avoid it
				continue
			}
			upperMounts, err := sn.mountsOf(ctx, upper)
			if err != nil {
				return errors.Wrapf(err, "failed to get mounts of upper %q", upper)
			}

			var lowerMounts []mount.Mount
			if lower != "" {
				var err error
				lowerMounts, err = sn.mountsOf(ctx, lower)
				if err != nil {
					return errors.Wrapf(err, "failed to get mounts of lower %q", lower)
				}
			}

			commitID := identity.NewID()
			if upper == applyKeys[len(applyKeys)-1] {
				// on the last iteration, commit to the caller-specified key
				commitID = key
			}
			if err := sn.Prepare(ctx, commitID, parentID); err != nil { // TODO opts?
				return errors.Wrapf(err, "failed to prepare %q as %q", parentID, commitID)
			}
			applyMountable, err := sn.Mounts(ctx, commitID)
			if err != nil {
				return errors.Wrapf(err, "failed to get mounts of %q", commitID)
			}
			applyMounts, unmount, err := applyMountable.Mount()
			if err != nil {
				return errors.Wrapf(err, "failed to mount %q", commitID)
			}

			desc, err := sn.differ.Compare(ctx, lowerMounts, upperMounts, diff.WithMediaType(ocispec.MediaTypeImageLayer))
			if err != nil {
				if err := unmount(); err != nil {
					bklog.G(ctx).Errorf("failed to unmount %s: %v", commitID, err)
				}
				return errors.Wrapf(err, "failed to compare lower %q and upper %q", lower, upper)
			}
			if _, err := sn.applier.Apply(ctx, desc, applyMounts); err != nil {
				if err := unmount(); err != nil {
					bklog.G(ctx).Errorf("failed to unmount %s: %v", commitID, err)
				}
				return errors.Wrapf(err, "failed to apply descriptor %q to %q", desc.Digest, commitID)
			}

			if err := unmount(); err != nil {
				return errors.Wrapf(err, "failed to unmount %q", commitID)
			}
			if err := sn.Commit(ctx, commitID); err != nil { // TODO opts?
				return errors.Wrapf(err, "failed to commit %q", commitID)
			}

			parentID = commitID
		}
		if err := sn.setMergeParents(ctx, key, parents); err != nil {
			return errors.Wrapf(err, "failed to set merge parents to %v", parents)
		}
		return nil
	}

	// TODO calculate the actual mount here and store it as a label instead of doing it at the mount call?
	if err := sn.Prepare(ctx, key, "", opts...); err != nil {
		return errors.Wrap(err, "failed to prepare new merge snapshot")
	}
	if err := sn.Commit(ctx, key, opts...); err != nil {
		return errors.Wrap(err, "failed to commit new merge snapshot")
	}
	// TODO make setParents be an opt to Commit instead?
	if err := sn.setMergeParents(ctx, key, parents); err != nil {
		return errors.Wrapf(err, "failed to set merge parents to %v", parents)
	}
	return nil
}

func (sn *fromContainerd) AddLease(ctx context.Context, key string) error {
	l, ok := leases.FromContext(ctx)
	if !ok {
		return fmt.Errorf("no lease to add %s to", key)
	}

	if err := sn.leaseManager.AddResource(ctx, leases.Lease{ID: l}, leases.Resource{
		ID:   sn.activeKey(key),
		Type: "snapshots/" + sn.Name(),
	}); err != nil {
		return err
	}
	if err := sn.leaseManager.AddResource(ctx, leases.Lease{ID: l}, leases.Resource{
		ID:   sn.commitKey(key),
		Type: "snapshots/" + sn.Name(),
	}); err != nil {
		return err
	}

	if _, err := sn.leaseManager.Create(ctx, func(viewl *leases.Lease) error {
		viewl.ID = l + "-view"
		viewl.Labels = map[string]string{
			"containerd.io/gc.flat": time.Now().UTC().Format(time.RFC3339Nano),
		}
		return nil
	}, leaseutil.MakeTemporary); err != nil && !errdefs.IsAlreadyExists(err) {
		return err
	}
	if err := sn.leaseManager.AddResource(ctx, leases.Lease{ID: l + "-view"}, leases.Resource{
		ID:   sn.viewKey(key),
		Type: "snapshots/" + sn.Name(),
	}); err != nil {
		return err
	}

	return nil
}

func (sn *fromContainerd) RemoveLease(ctx context.Context, key string) error {
	l, ok := leases.FromContext(ctx)
	if !ok {
		return fmt.Errorf("no lease to add %s to", key)
	}

	if err := sn.leaseManager.DeleteResource(ctx, leases.Lease{ID: l}, leases.Resource{
		ID:   key,
		Type: "snapshots/" + sn.Name(),
	}); err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	if err := sn.leaseManager.DeleteResource(ctx, leases.Lease{ID: l}, leases.Resource{
		ID:   key,
		Type: "snapshots/" + sn.Name(),
	}); err != nil && !errdefs.IsNotFound(err) {
		return err
	}

	return sn.RemoveViewLease(ctx, key)
}

func (sn *fromContainerd) RemoveViewLease(ctx context.Context, key string) error {
	l, ok := leases.FromContext(ctx)
	if !ok {
		return fmt.Errorf("no lease to add %s view to", key)
	}

	if err := sn.leaseManager.Delete(ctx, leases.Lease{ID: l + "-view"}); err != nil && !errdefs.IsNotFound(err) {
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
