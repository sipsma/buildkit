package snapshot

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/docker/docker/pkg/idtools"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/flightcontrol"
	"github.com/moby/buildkit/util/leaseutil"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type Mountable interface {
	Mount() ([]mount.Mount, func() error, error)
	IdentityMapping() *idtools.IdentityMapping
}

// Snapshotter defines interface that any snapshot implementation should satisfy
type Snapshotter interface {
	Name() string
	Mounts(ctx context.Context, key string) (Mountable, error)
	Prepare(ctx context.Context, key string, parent string, opts ...snapshots.Opt) error
	View(ctx context.Context, key string, parent string, opts ...snapshots.Opt) error
	Merge(ctx context.Context, key string, parents []string, opts ...snapshots.Opt) error

	Stat(ctx context.Context, key string) (snapshots.Info, error)
	Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error)
	Usage(ctx context.Context, key string) (snapshots.Usage, error)
	Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error
	Walk(ctx context.Context, fn snapshots.WalkFunc, filters ...string) error
	Close() error
	IdentityMapping() *idtools.IdentityMapping
	// Containerd returns the underlying containerd snapshotter interface used by this snapshotter
	Containerd() snapshots.Snapshotter
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
	lm leases.Manager,
	rootless bool,
) Snapshotter {
	var useOverlayMerge bool
	for _, sn := range overlayMergeSnapshotters {
		if name == sn {
			useOverlayMerge = true
			break
		}
	}
	var userxattr bool
	if useOverlayMerge {
		var err error
		userxattr, err = needsUserXAttr(ctx, s, lm)
		if err != nil {
			bklog.G(ctx).Errorf("failed to check userxattr, defaulting to %t: %v", userxattr, err)
		}
	}

	if rootless && useOverlayMerge && !userxattr {
		// This can only happen on pre-5.11 kernels that are patched to allow overlay mounts from non-root
		// user namespaces without support for userxattr. These kernels are problematic because they use
		// privileged xattrs to track opacity but such xattrs are not visible inside the user namespace.
		// Therefore, we disable overlay-based merges in this case and fallback to copy-based merges.
		useOverlayMerge = false
	}

	var useHardlinks bool
	for _, sn := range hardlinkMergeSnapshotters {
		if name == sn {
			useHardlinks = true
			break
		}
	}

	return &fromContainerd{
		name:            name,
		snapshotter:     s,
		idmap:           idmap,
		useOverlayMerge: useOverlayMerge,
		userxattr:       userxattr,
		useHardlinks:    useHardlinks,
		lm:              lm,
	}
}

type fromContainerd struct {
	name            string
	snapshotter     snapshots.Snapshotter
	idmap           *idtools.IdentityMapping
	useOverlayMerge bool
	userxattr       bool
	useHardlinks    bool
	lm              leases.Manager
	mergeG          flightcontrol.Group
	checkOpaqueG    flightcontrol.Group
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
	return sn.snapshotter.Stat(ctx, key)
}

func (sn *fromContainerd) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	info, err := sn.Stat(ctx, key)
	if err != nil {
		return snapshots.Usage{}, err
	}
	if isMergeView(info) {
		key = info.Parent
		if key == "" {
			return snapshots.Usage{}, nil
		}
		if info, err := sn.Stat(ctx, key); err != nil {
			return snapshots.Usage{}, err
		} else if usage, ok, err := mergeUsageOf(info); err != nil {
			return snapshots.Usage{}, err
		} else if ok {
			return usage, nil
		}
	}
	return sn.snapshotter.Usage(ctx, key)
}

func (sn *fromContainerd) Mounts(ctx context.Context, key string) (_ Mountable, rerr error) {
	if !sn.useOverlayMerge {
		mounts, err := sn.snapshotter.Mounts(ctx, key)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get mounts")
		}
		return &staticMountable{mounts: mounts, idmap: sn.idmap, id: key}, nil
	}

	info, err := sn.Stat(ctx, key)
	if err != nil {
		return nil, err
	}
	mergeKeys := mergeKeysOf(info)
	if len(mergeKeys) == 0 {
		mounts, err := sn.snapshotter.Mounts(ctx, key)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get mounts")
		}
		if err := sn.fixOpaqueDirs(ctx, key, mounts); err != nil {
			return nil, errors.Wrapf(err, "failed to fix opaque dirs of %s", key)
		}
		// still use mergedOverlay in this case to get the deduplication and removal of unneeded lowerdirs
		return mergedOverlay{parents: mounts, idmap: sn.idmap, userxattr: sn.userxattr}, nil
	}

	ctx, done, err := leaseutil.WithLease(ctx, sn.lm, leaseutil.MakeTemporary)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create temporary lease for view mounts during merge")
	}
	defer func() {
		if rerr != nil {
			done(context.TODO())
		}
	}()

	merged := mergedOverlay{idmap: sn.idmap, userxattr: sn.userxattr, cleanup: func() error { return done(context.TODO()) }}

	for _, mergeKey := range mergeKeysOf(info) {
		viewID := identity.NewID()
		mounts, err := sn.snapshotter.View(ctx, viewID, mergeKey)
		if err != nil {
			return nil, err
		}
		if err := sn.fixOpaqueDirs(ctx, viewID, mounts); err != nil {
			return nil, errors.Wrapf(err, "failed to fix opaque dirs of %s", mergeKey)
		}
		merged.parents = append(merged.parents, mounts...)
	}
	if !isMergeView(info) {
		// if key isn't a merge view, we should include its mounts on top of the keys merged below it
		mounts, err := sn.snapshotter.Mounts(ctx, key)
		if err != nil {
			return nil, err
		}
		if err := sn.fixOpaqueDirs(ctx, key, mounts); err != nil {
			return nil, errors.Wrapf(err, "failed to fix opaque dirs of %s", key)
		}
		merged.parents = append(merged.parents, mounts...)
	}
	return merged, nil
}

// caller should hold temporary lease
func (sn *fromContainerd) fixOpaqueDirs(ctx context.Context, key string, mounts []mount.Mount) error {
	_, err := sn.checkOpaqueG.Do(ctx, key, func(ctx context.Context) (interface{}, error) {
		info, err := sn.Stat(ctx, key)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to stat key %q", key)
		}
		if _, ok := info.Labels[checkedOpaqueLabel]; ok || info.Parent == "" {
			return nil, nil
		}

		eg, ctx := errgroup.WithContext(ctx)
		if info.Parent != "" {
			eg.Go(func() error {
				if err := sn.fixOpaqueDirs(ctx, info.Parent, nil); err != nil {
					return errors.Wrapf(err, "failed to fix opaque dir of parent %s", info.Parent)
				}
				return nil
			})
		}

		if info.Kind != snapshots.KindActive {
			eg.Go(func() error {
				if mounts == nil {
					if info.Kind != snapshots.KindView {
						tempViewID := identity.NewID()
						ms, err := sn.snapshotter.View(ctx, tempViewID, key)
						if err != nil {
							return errors.Wrapf(err, "failed to create view %s for key %q", tempViewID, key)
						}
						mounts = ms
					} else {
						ms, err := sn.snapshotter.Mounts(ctx, key)
						if err != nil {
							return errors.Wrapf(err, "failed to get mounts for key %q", key)
						}
						mounts = ms
					}
				}
				if err := fixOpaqueDirs(ctx, mounts, sn.userxattr); err != nil {
					return err
				}
				if _, err := sn.Update(ctx, snapshots.Info{
					Name: key,
					Labels: map[string]string{
						checkedOpaqueLabel: "y",
					},
				}, "labels."+checkedOpaqueLabel); err != nil {
					return errors.Wrapf(err, "failed to update checked opaque label for key %q", key)
				}
				return nil
			})
		}

		return nil, eg.Wait()
	})
	return err
}

func (sn *fromContainerd) Prepare(ctx context.Context, key string, parent string, opts ...snapshots.Opt) error {
	var parentInfo snapshots.Info
	if parent != "" {
		var err error
		parentInfo, err = sn.Stat(ctx, parent)
		if err != nil {
			return errors.Wrap(err, "failed to stat parent")
		}
	}

	if !sn.useOverlayMerge {
		if isMergeView(parentInfo) {
			// if parent is a merge view, use the underlying committed snapshot as the parent to prepare from
			parent = parentInfo.Parent
		}
		_, err := sn.snapshotter.Prepare(ctx, key, parent, opts...)
		return err
	}

	var mergeKeys []string
	switch parentInfo.Kind {
	case snapshots.KindCommitted:
		mergeKeys = mergeKeysOf(parentInfo)
	case snapshots.KindView:
		if !isMergeView(parentInfo) {
			return fmt.Errorf("cannot prepare key %s on top of parent %q of type %s", key, parent, parentInfo.Kind)
		}
		// If parent is a merge view, then this snapshot won't have an actual parent. It will just be merged on top
		// of its merge keys.
		parent = ""
		mergeKeys = mergeKeysOf(parentInfo)
	case snapshots.KindActive:
		return fmt.Errorf("cannot prepare key %s on top of parent %q of type %s", key, parent, parentInfo.Kind)
	case snapshots.KindUnknown:
		// no-op, there is no parent
	}

	if parent == "" {
		// There is a corner case with having a single layer snapshot in the efficient merge case where you
		// merge the active snapshot resulting in the need to return an overlay mount. However, because single
		// layer snapshots are always returned as bind-mounts (due to overlayfs limitations), we don't have a
		// workdir in that case. The workaround right now is to just create a dummy base layer so you always
		// end up with at least 2 layers here and thus have a workdir. The dummy base layers are optimized out
		// in the actual mount because empty lowerdirs are removed (which is a good optimization to have either way).
		ctx, done, err := leaseutil.WithLease(ctx, sn.lm, leaseutil.MakeTemporary)
		if err != nil {
			return errors.Wrap(err, "failed to make temporary lease during prepare")
		}
		defer done(context.TODO())

		parentPrepare := identity.NewID()
		if _, err := sn.snapshotter.Prepare(ctx, parentPrepare, ""); err != nil {
			return errors.Wrap(err, "failed to prepare base parent for overlay merge")
		}
		parent = identity.NewID()
		if err := sn.snapshotter.Commit(ctx, parent, parentPrepare); err != nil {
			return errors.Wrap(err, "failed to commit base parent for overlay merge")
		}
	}

	opts = append(opts, withMergeKeys(mergeKeys))
	_, err := sn.snapshotter.Prepare(ctx, key, parent, opts...)
	return err
}

func (sn *fromContainerd) View(ctx context.Context, key string, parent string, opts ...snapshots.Opt) error {
	return sn.view(ctx, key, []string{parent}, opts...)
}

func (sn *fromContainerd) Merge(ctx context.Context, key string, parents []string, opts ...snapshots.Opt) error {
	return sn.view(ctx, key, parents, append(opts, withIsMergeView())...)
}

func (sn *fromContainerd) view(ctx context.Context, key string, parents []string, opts ...snapshots.Opt) error {
	var filteredParents []snapshots.Info // filter out "" parents
	var mergeKeys []string
	for _, p := range parents {
		if p == "" {
			continue
		}
		info, err := sn.Stat(ctx, p)
		if err != nil {
			return errors.Wrapf(err, "failed to stat parent during merge of %s", key)
		}
		filteredParents = append(filteredParents, info)
		switch info.Kind {
		case snapshots.KindCommitted:
			// if parent is a committed snapshot, we want to merge on top of its merge parents and it itself
			mergeKeys = append(mergeKeys, mergeKeysOf(info)...)
			mergeKeys = append(mergeKeys, p)
		case snapshots.KindView:
			if !isMergeView(info) {
				return fmt.Errorf("cannot merge key %s on top of parent %q of type %s", key, p, info.Kind)
			}
			// if parent is a just a merge view, we only want to merge on top of its merge parents
			mergeKeys = append(mergeKeys, mergeKeysOf(info)...)
		case snapshots.KindActive, snapshots.KindUnknown:
			return fmt.Errorf("cannot merge key %s on top of parent %q of type %s", key, p, info.Kind)
		}
	}

	opts = append(opts, withMergeKeys(mergeKeys))

	if len(filteredParents) == 0 {
		// just create an empty view
		_, err := sn.snapshotter.View(ctx, key, "", opts...)
		return err
	}

	if len(filteredParents) == 1 {
		// if there's only one parent, then just create a view directly from it as there's nothing to merge it with
		parent := filteredParents[0].Name
		if isMergeView(filteredParents[0]) {
			parent = filteredParents[0].Parent
		}
		_, err := sn.snapshotter.View(ctx, key, parent, opts...)
		return err
	}

	if !sn.useOverlayMerge {
		mergedID, err := sn.diffApplyMerge(ctx, mergeKeys)
		if err != nil {
			return err
		}
		_, err = sn.snapshotter.View(ctx, key, mergedID, opts...)
		return err
	}

	_, err := sn.snapshotter.View(ctx, key, "", opts...)
	return err
}

func (sn *fromContainerd) diffApplyMerge(ctx context.Context, parents []string) (string, error) {
	var applyChains [][]string

	// flatten parents into each key in their chain
	for _, parent := range parents {
		var chain []string
		for curkey := parent; curkey != ""; {
			info, err := sn.Stat(ctx, curkey)
			if err != nil {
				return "", errors.Wrapf(err, "failed to stat chain key %q", curkey)
			}
			chain = append(chain, info.Name)
			curkey = info.Parent
		}
		applyChains = append(applyChains, chain)
	}

	// remove duplicates so that only the uppermost occurrence of a snapshot is applied
	memo := make(map[string]struct{})
	var filteredChains [][]string
	digester := sha256.New()
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
			if _, err := digester.Write([]byte(key)); err != nil {
				return "", errors.Wrapf(err, "failed to write snapshot key %q to hash", key)
			}
		}
		if filteredChain != nil {
			filteredChain = append(filteredChain, "") // include empty layer as the base of each chain
			filteredChains = append(filteredChains, filteredChain)
		}
	}
	applyChains = filteredChains

	key := (&big.Int{}).SetBytes(digester.Sum(nil)).Text(36)[:36] // use same format as identity.NewID()
	if info, err := sn.Stat(ctx, key); err == nil {
		if info.Kind == snapshots.KindCommitted {
			return key, nil
		}
		bklog.G(ctx).Debugf("snapshot %q is in invalid state %s, not reusing for merge", key, info.Kind.String())
		key = identity.NewID()
	} else if !errdefs.IsNotFound(err) {
		return "", err
	}

	_, err := sn.mergeG.Do(ctx, key, func(ctx context.Context) (interface{}, error) {
		// the first parent will be the top of the chain we apply the rest to
		parentInfo, err := sn.Stat(ctx, applyChains[len(applyChains)-1][0])
		if err != nil {
			return nil, err
		}
		parentID := parentInfo.Name
		applyChains = applyChains[:len(applyChains)-1]

		prepareKey := identity.NewID()
		if _, err := sn.snapshotter.Prepare(ctx, prepareKey, parentID); err != nil {
			return nil, errors.Wrapf(err, "failed to prepare %q as %q", parentID, key)
		}
		applyMountable, err := sn.Mounts(ctx, prepareKey)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get mounts of %q", key)
		}
		applyMounts, unmount, err := applyMountable.Mount()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to mount %q", key)
		}

		tempLeaseCtx, done, err := leaseutil.WithLease(ctx, sn.lm, leaseutil.MakeTemporary)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create temporary lease for view mounts during merge")
		}
		defer done(context.TODO())

		// externalHardlinks keeps track of which inodes have been hard-linked between snapshots (which is enabled when
		// sn.useHardlinks is set to true)
		externalHardlinks := make(map[uint64]struct{})

		// iterate over (lower, upper) pairs in each applyChain, calculating their diffs and applying each one to the mount
		// the chains had to be constructed in reverse order (child->parent), so iterate in reverse
		for i := range applyChains {
			chain := applyChains[len(applyChains)-1-i]
			for j := range chain[:len(chain)-1] {
				lower := chain[len(chain)-1-j]
				upper := chain[len(chain)-2-j]

				var lowerMounts []mount.Mount
				if lower != "" {
					viewID := identity.NewID()
					var err error
					lowerMounts, err = sn.snapshotter.View(tempLeaseCtx, viewID, lower)
					if err != nil {
						return nil, errors.Wrapf(err, "failed to get mounts of lower %q", lower)
					}
				}

				viewID := identity.NewID()
				upperMounts, err := sn.snapshotter.View(tempLeaseCtx, viewID, upper)
				if err != nil {
					return nil, errors.Wrapf(err, "failed to get mounts of upper %q", upper)
				}

				err = diffApply(ctx, lowerMounts, upperMounts, applyMounts, sn.useHardlinks, externalHardlinks)
				if err != nil {
					return nil, err
				}
			}
		}

		usage, err := diskUsage(ctx, applyMounts, externalHardlinks)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get disk usage of diff apply merge")
		}
		if err := unmount(); err != nil {
			return nil, errors.Wrapf(err, "failed to unmount %q", key)
		}
		if err := sn.snapshotter.Commit(ctx, key, prepareKey, withMergeUsage(usage)); err != nil {
			return nil, errors.Wrapf(err, "failed to commit %q", key)
		}
		return nil, nil
	})
	if err != nil {
		return "", err
	}
	return key, nil
}

func (sn *fromContainerd) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	info, err := sn.Stat(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to stat active key during commit")
	}
	opts = append(opts, snapshots.WithLabels(snapshots.FilterInheritedLabels(info.Labels)))
	return sn.snapshotter.Commit(ctx, name, key, opts...)
}

func (sn *fromContainerd) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	return sn.snapshotter.Update(ctx, info, fieldpaths...)
}

func (sn *fromContainerd) Walk(ctx context.Context, fn snapshots.WalkFunc, filters ...string) error {
	return sn.snapshotter.Walk(ctx, fn, filters...)
}

// checkedOpaqueLabel is used to indicate that a snapshot has already been checked for any unnecessary opaque dirs
const checkedOpaqueLabel = "buildkit.checkedOpaque"

// mergeUsage{Size,Inodes}Label hold the correct usage calculations for diffApplyMerges, for which the builtin usage
// is wrong because it can't account for hardlinks made across immutable snapshots
const mergeUsageSizeLabel = "buildkit.mergeUsageSize"
const mergeUsageInodesLabel = "buildkit.mergeUsageInodes"

func withMergeUsage(usage snapshots.Usage) snapshots.Opt {
	return snapshots.WithLabels(map[string]string{
		mergeUsageSizeLabel:   strconv.Itoa(int(usage.Size)),
		mergeUsageInodesLabel: strconv.Itoa(int(usage.Inodes)),
	})
}

func mergeUsageOf(info snapshots.Info) (usage snapshots.Usage, ok bool, rerr error) {
	if info.Labels == nil {
		return snapshots.Usage{}, false, nil
	}
	if str, ok := info.Labels[mergeUsageSizeLabel]; ok {
		i, err := strconv.Atoi(str)
		if err != nil {
			return snapshots.Usage{}, false, err
		}
		usage.Size = int64(i)
	}
	if str, ok := info.Labels[mergeUsageInodesLabel]; ok {
		i, err := strconv.Atoi(str)
		if err != nil {
			return snapshots.Usage{}, false, err
		}
		usage.Inodes = int64(i)
	}
	return usage, true, nil
}

// isMergeViewLabel is set on view snapshots that are created as views of merged snapshots
const isMergeViewLabel = "buildkit.isMergeView"

func withIsMergeView() snapshots.Opt {
	return snapshots.WithLabels(map[string]string{
		isMergeViewLabel: "y",
	})
}

func isMergeView(info snapshots.Info) bool {
	return info.Kind == snapshots.KindView &&
		info.Labels != nil &&
		info.Labels[isMergeViewLabel] != ""
}

// mergeKeysLabel holds the keys that a given snapshot should be merged on top of in order to be mounted
const mergeKeysLabel = "containerd.io/snapshot/buildkit.mergeKeys"

func withMergeKeys(keys []string) snapshots.Opt {
	return snapshots.WithLabels(map[string]string{
		mergeKeysLabel: strings.Join(keys, ","),
	})
}

func mergeKeysOf(info snapshots.Info) []string {
	// TODO:(sipsma) consider validating that keys don't have ","
	if v := info.Labels[mergeKeysLabel]; v != "" {
		return strings.Split(v, ",")
	}
	return nil
}
