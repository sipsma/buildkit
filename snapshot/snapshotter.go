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
)

type Mountable interface {
	Mount() ([]mount.Mount, func() error, error)
	IdentityMapping() *idtools.IdentityMapping
}

// Snapshotter defines interface that any snapshot implementation should satisfy
type Snapshotter interface {
	Name() string
	Mounts(ctx context.Context, key string) (Mountable, error)

	// Prepare is the same as the underlying containerd API but with the additional feature that
	// parent can be a key created by a call to Merge.
	Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) error

	View(ctx context.Context, key string, parent string, opts ...snapshots.Opt) error

	// Merge creates a snapshot whose contents are the merged contents of each of the provided
	// parent keys. This merged snapshot is equivalent to the result of taking each layer of the
	// provided parent snapshots and applying them on top of each other, in the order provided,
	// lowest->highest.
	//
	// Each parent key is expected to be either a committed snapshot or a key created by a previous
	// call to Merge. The snapshot created by Merge is immutable. A mutable merged snapshot can be
	// created by providing a key created by Merge as the parent to a Prepare call.
	//
	// Merges are stored as Views in the underlying snapshotter with special labels that indicate
	// which keys they are merging together, which may be needed by the Mount method to know how
	// to construct the mount for the merge. These labels are passed onto any children created from
	// the merge key in other Prepare or Merge calls. Creating merges as Views rather than just
	// committed snapshots is preferable in that it avoids extraneous calls to Prepare+Commit in many
	// cases and lets callers get mounts for the merge directly rather than create a View from it
	// first.
	//
	// The implementation of Merge depends on the underlying snapshotter being used:
	// * The basic implementation used for any generic snapshotter will create a merged snapshot
	//   by actually applying each layer making up the merge to a new initially empty snapshot.
	//   Some snapshotters, such as the native snapshotter, may be able to optimize this by using
	//   hardlinks to skip copying non-directories. See the diffApply functions for more details.
	// * Overlay-based snapshotters can optimize further by simply joining together bind+overlay
	//   mounts into a single overlay mount. They therefore don't need to use any extra space to
	//   create merged snapshots. The Views underlying these merges are empty and only exist in
	//   order to store the labels that tell the Mount method which keys should be merged together
	//   to create the final mount.
	// The size of a merged snapshot (as returned by the Usage method) will thus depend on the merge
	// implementation. It is 0 when optimized overlay merges are enabled and non-zero for other cases,
	// with the exact value depending on whether hardlinks were enabled.
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

// hardlinkMergeSnapshotters are the names of snapshotters that support merges implemented by
// creating "hardlink farms" where non-directory objects are hard-linked into the merged tree
// from their parent snapshots.
var hardlinkMergeSnapshotters = []string{
	"native",
	"overlayfs",
	"stargz",
}

func FromContainerdSnapshotter(
	ctx context.Context,
	name string,
	s snapshots.Snapshotter,
	idmap *idtools.IdentityMapping,
	lm leases.Manager,
	rootless bool,
) Snapshotter {
	var useHardlinks bool
	for _, sn := range hardlinkMergeSnapshotters {
		if name == sn {
			useHardlinks = true
			break
		}
	}

	// TODO: re-add fix for pre 5.11 kernels, we won't be able to see or set opaque if in them and are rootless

	return &fromContainerd{
		name:         name,
		snapshotter:  s,
		idmap:        idmap,
		useHardlinks: useHardlinks,
		lm:           lm,
	}
}

type fromContainerd struct {
	name         string
	snapshotter  snapshots.Snapshotter
	idmap        *idtools.IdentityMapping
	useHardlinks bool
	lm           leases.Manager
	mergeG       flightcontrol.Group
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
	if isMerge(info) {
		// If key was created by Merge, we may need to use the annotated mergeUsage key as
		// the snapshotter's usage method is wrong when hardlinks are used to create the merge.
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
	mounts, err := sn.snapshotter.Mounts(ctx, key)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get mounts")
	}
	return &staticMountable{mounts: mounts, idmap: sn.idmap, id: key}, nil
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

	if isMerge(parentInfo) {
		// If parent was created by Merge, it's stored as a view, so use the underlying committed key as
		// the parent to prepare from.
		parent = parentInfo.Parent
	}
	_, err := sn.snapshotter.Prepare(ctx, key, parent, opts...)
	return err
}

func (sn *fromContainerd) View(ctx context.Context, key string, parent string, opts ...snapshots.Opt) error {
	// just create a view from a single parent
	return sn.view(ctx, key, []string{parent}, opts...)
}

func (sn *fromContainerd) Merge(ctx context.Context, key string, parents []string, opts ...snapshots.Opt) error {
	return sn.view(ctx, key, parents, append(opts, withIsMerge())...)
}

// view creates a View from one or more parents. The View will have a label indicating which keys should be
// merged together to create its mount in case those are needed by the Mount method. In the case where a single
// parent is provided, it will behave mostly the same as the underlying View method but with those extra labels
// stored. If multiple parents are provided, then an actual merged snapshot may be created in the case where
// efficient overlay merges aren't possible.
func (sn *fromContainerd) view(ctx context.Context, key string, parents []string, opts ...snapshots.Opt) error {
	// Figure out which, if any, keys should be merged together to create this view. If a parent key consists of
	// a merge of keys, flatten those out here. Also filter out any empty parents.
	var mergeKeys []string
	var filteredParents []snapshots.Info
	for _, p := range parents {
		if p == "" {
			// skip any empty parents
			continue
		}
		info, err := sn.Stat(ctx, p)
		if err != nil {
			return errors.Wrap(err, "failed to stat parent while creating view")
		}
		filteredParents = append(filteredParents, info)
		switch info.Kind {
		case snapshots.KindCommitted:
			// If parent is a committed snapshot, this view will need to be merged on top of its merge keys in
			// addition to it itself.
			mergeKeys = append(mergeKeys, mergeKeysOf(info)...)
			mergeKeys = append(mergeKeys, p)
		case snapshots.KindView:
			if !isMerge(info) {
				return fmt.Errorf("cannot merge key %s on top of parent %q of type %s", key, p, info.Kind)
			}
			// If parent was directly created by Merge, we only want to merge on top of its merge parents as
			// it doesn't have any unique contents by itself.
			mergeKeys = append(mergeKeys, mergeKeysOf(info)...)
		case snapshots.KindActive, snapshots.KindUnknown:
			return fmt.Errorf("cannot merge key %s on top of parent %q of type %s", key, p, info.Kind)
		}
	}

	opts = append(opts, withMergeKeys(mergeKeys))

	if len(filteredParents) == 0 {
		// If there's no parents (or all of them were empty), just create an empty view.
		_, err := sn.snapshotter.View(ctx, key, "", opts...)
		return err
	}

	if len(filteredParents) == 1 {
		// If there's only one parent, then just create a view directly from it as there's nothing to merge it with.
		parent := filteredParents[0].Name
		if isMerge(filteredParents[0]) {
			parent = filteredParents[0].Parent
		}
		_, err := sn.snapshotter.View(ctx, key, parent, opts...)
		return err
	}

	// Create a new snapshot with actual contents by applying the merged snapshots on top of each other.
	mergedID, err := sn.diffApplyMerge(ctx, mergeKeys)
	if err != nil {
		return err
	}
	_, err = sn.snapshotter.View(ctx, key, mergedID, opts...)
	return err
}

// diffApplyMerge creates a new snapshot by apply the layers making up each parent on top of each other. Parents
// are expected to be provided in lowest->highest order. When applying layers, they aren't applied directly; instead
// their diff from their parent is calculated and applied. This ensures that deletions are applied correctly in a
// snapshotter agnostic way.
//
// Depending on the underlying snapshotter, hardlinks may be used instead of copying files. This saves the merged
// snapshotter from taking up extra disk space and inodes other than those for directories (which can't be hard-linked).
func (sn *fromContainerd) diffApplyMerge(ctx context.Context, parents []string) (string, error) {
	// Map each key in parents to each key making up the snapshot chain, ordered from highest->lowest due to
	// the fact that you have to traverse snapshots from child->parent.
	var applyChains [][]string
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

	// Remove duplicates so that only the uppermost occurrence of a snapshot is applied. Any dupes besides
	// the uppermost can't have any effect on the final merged snapshot and thus can be skipped.
	// Keep a running hash of the keys that will make up the final merge, which will be used to create a
	// content-aware key for this snapshot and thus de-dupe it if any equivalents have already been made.
	memo := make(map[string]struct{})
	var filteredChains [][]string
	digester := sha256.New()
	for i := range applyChains {
		// iterate over applyChains backwards because we need to go highest->lowest when de-duping in order
		// to keep the highest occurrence of a key.
		chain := applyChains[len(applyChains)-1-i]
		var filteredChain []string
		for _, key := range chain {
			if _, ok := memo[key]; ok {
				// already included this key in the final apply chain, skip this dupe of it
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

	// Calculate the key we should use for the new snapshot via the hash of the keys that will be applied to
	// create the merge. If there's already a snapshot with this key, then we can just re-use it rather than
	// create a duplicate of it.
	key := (&big.Int{}).SetBytes(digester.Sum(nil)).Text(36)[:36]
	if info, err := sn.Stat(ctx, key); err == nil {
		if info.Kind == snapshots.KindCommitted {
			return key, nil
		}
		bklog.G(ctx).Debugf("snapshot %q is in invalid state %s, not reusing for merge", key, info.Kind.String())
		key = identity.NewID()
	} else if !errdefs.IsNotFound(err) {
		return "", err
	}

	// Create the merge in a flightcontrol group for the key, ensuring parallel equivalent calls get de-duped
	_, err := sn.mergeG.Do(ctx, key, func(ctx context.Context) (interface{}, error) {
		// Make a new empty snapshot which will be merged into
		prepareKey := identity.NewID()
		if _, err := sn.snapshotter.Prepare(ctx, prepareKey, ""); err != nil {
			return nil, errors.Wrapf(err, "failed to prepare %q", key)
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

		// externalHardlinks keeps track of which inodes have been hard-linked between snapshots (which is
		//	enabled when sn.useHardlinks is set to true)
		externalHardlinks := make(map[uint64]struct{})

		// Iterate over (lower, upper) pairs in each applyChain, calculating their diffs and applying each
		// one to the mount the chains had to be constructed in reverse order (child->parent), so iterate
		// in reverse.
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

		// save the correctly calculated usage as a label on the committed key
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

// isMergeLabel is set on view snapshots that are merged snapshots
const isMergeLabel = "buildkit.isMerge"

func withIsMerge() snapshots.Opt {
	return snapshots.WithLabels(map[string]string{
		isMergeLabel: "y",
	})
}

func isMerge(info snapshots.Info) bool {
	return info.Kind == snapshots.KindView &&
		info.Labels != nil &&
		info.Labels[isMergeLabel] != ""
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
