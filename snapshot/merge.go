package snapshot

import (
	"context"
	"strconv"

	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/pkg/userns"
	"github.com/containerd/containerd/snapshots"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/leaseutil"
	"github.com/pkg/errors"
)

// hardlinkMergeSnapshotters are the names of snapshotters that support merges implemented by
// creating "hardlink farms" where non-directory objects are hard-linked into the merged tree
// from their parent snapshots.
var hardlinkMergeSnapshotters = map[string]struct{}{
	"native":    {},
	"overlayfs": {},
	"stargz":    {},
}

// overlayBasedSnapshotters are the names of snapshotter that use overlay mounts, which
// enables optimizations such as skipping the base layer when doing a hardlink merge.
var overlayBasedSnapshotters = map[string]struct{}{
	"overlayfs": {},
	"stargz":    {},
}

type Diff struct {
	Lower string
	Upper string
}

type MergeSnapshotter interface {
	Snapshotter
	// Merge creates a snapshot whose contents are the merged contents of each of the provided
	// parent keys. This merged snapshot is equivalent to the result of taking each layer of the
	// provided parent snapshots and applying them on top of each other, in the order provided,
	// lowest->highest.
	//
	// Each parent key is expected to be either a committed snapshot. The snapshot created by
	// Merge is also committed.
	//
	// The size of a merged snapshot (as returned by the Usage method) depends on the merge
	// implementation. Implementations using hardlinks to create merged views will take up
	// less space than those that use copies, for example.
	// TODO: update docs
	Merge(ctx context.Context, key string, diffs []Diff, opts ...snapshots.Opt) error
}

type mergeSnapshotter struct {
	Snapshotter
	lm leases.Manager

	// Whether we should try to implement merges by hardlinking between underlying directories
	useHardlinks bool

	// Whether the snapshotter is overlay-based, which enables some required behavior like
	// creation of whiteout devices to represent deletes in addition to some optimizations
	// like using the first merge input as the parent snapshot.
	overlayBased bool
}

func NewMergeSnapshotter(ctx context.Context, sn Snapshotter, lm leases.Manager) MergeSnapshotter {
	name := sn.Name()
	_, useHardlinks := hardlinkMergeSnapshotters[name]
	_, overlayBased := overlayBasedSnapshotters[name]

	if overlayBased && userns.RunningInUserNS() {
		// When using an overlay-based snapshotter, if we are running rootless on a pre-5.11
		// kernel, we will not have userxattr. This results in opaque xattrs not being visible
		// to us and thus breaking the overlay-optimized differ. This also means that there are
		// cases where in order to apply a deletion, we'd need to create a whiteout device but
		// may not have access to one to hardlink, so we just fall back to not using hardlinks
		// at all in this case.
		userxattr, err := needsUserXAttr(ctx, sn, lm)
		if err != nil {
			bklog.G(ctx).Debugf("failed to check user xattr: %v", err)
			useHardlinks = false
		} else {
			useHardlinks = userxattr
		}
	}

	return &mergeSnapshotter{
		Snapshotter:  sn,
		lm:           lm,
		useHardlinks: useHardlinks,
		overlayBased: overlayBased,
	}
}

func (sn *mergeSnapshotter) Merge(ctx context.Context, key string, diffs []Diff, opts ...snapshots.Opt) error {
	var expandedDiffs []Diff
	for _, diff := range diffs {
		subdiffs, err := sn.expandDiff(ctx, diff)
		if err != nil {
			return err
		}
		expandedDiffs = append(expandedDiffs, subdiffs...)
	}
	diffs = expandedDiffs

	var baseKey string
	if sn.overlayBased {
		// Overlay-based snapshotters can skip the base snapshot of the merge (if one exists) and just use it as the
		// parent of the merge snapshot. Other snapshotters will start empty (with baseKey set to "").
		// Find the baseKey by following the chain of diffs for as long as it follows the pattern of the current lower
		// being the parent of the current upper and equal to the previous upper.
		var baseIndex int
		for i, diff := range diffs {
			info, err := sn.Stat(ctx, diff.Upper)
			if err != nil {
				return err
			}
			if info.Parent != diff.Lower {
				break
			}
			if diff.Lower != baseKey {
				break
			}
			baseKey = diff.Upper
			baseIndex = i + 1
		}
		diffs = diffs[baseIndex:]
	}

	// Make the snapshot that will be merged into
	prepareKey := identity.NewID()
	if err := sn.Prepare(ctx, prepareKey, baseKey); err != nil {
		return errors.Wrapf(err, "failed to prepare %q", key)
	}
	applyMountable, err := sn.Mounts(ctx, prepareKey)
	if err != nil {
		return errors.Wrapf(err, "failed to get mounts of %q", key)
	}

	tempLeaseCtx, done, err := leaseutil.WithLease(ctx, sn.lm, leaseutil.MakeTemporary)
	if err != nil {
		return errors.Wrap(err, "failed to create temporary lease for view mounts during merge")
	}
	defer done(context.TODO())

	// externalHardlinks keeps track of which inodes have been hard-linked between snapshots (which is
	//	enabled when sn.useHardlinks is set to true)
	externalHardlinks := make(map[uint64]struct{})

	// Iterate over (lower, upper) pairs in each applyChain, calculating their diffs and applying each
	// one to the mount.
	// TODO: note for @tonistiigi, this is where we are currently flattening input snapshots for both
	// native and overlay implementations. What is the advantage of splitting this into separate snapshots
	// for the overlay case?
	for _, diff := range diffs {
		var lowerMountable Mountable
		if diff.Lower != "" {
			viewID := identity.NewID()
			var err error
			lowerMountable, err = sn.View(tempLeaseCtx, viewID, diff.Lower)
			if err != nil {
				return errors.Wrapf(err, "failed to get mounts of lower %q", diff.Lower)
			}
		}

		viewID := identity.NewID()
		upperMountable, err := sn.View(tempLeaseCtx, viewID, diff.Upper)
		if err != nil {
			return errors.Wrapf(err, "failed to get mounts of upper %q", diff.Upper)
		}

		err = diffApply(tempLeaseCtx, lowerMountable, upperMountable, applyMountable, sn.useHardlinks, externalHardlinks, sn.overlayBased)
		if err != nil {
			return err
		}
	}

	// save the correctly calculated usage as a label on the committed key
	usage, err := diskUsage(ctx, applyMountable, externalHardlinks)
	if err != nil {
		return errors.Wrap(err, "failed to get disk usage of diff apply merge")
	}
	if err := sn.Commit(ctx, key, prepareKey, withMergeUsage(usage)); err != nil {
		return errors.Wrapf(err, "failed to commit %q", key)
	}
	return nil
}

// TODO: doc
func (sn *mergeSnapshotter) expandDiff(ctx context.Context, diff Diff) ([]Diff, error) {
	var diffs []Diff
	var key string
	for key = diff.Upper; key != "" && key != diff.Lower; {
		info, err := sn.Stat(ctx, key)
		if err != nil {
			return nil, err
		}
		diffs = append(diffs, Diff{
			Lower: info.Parent,
			Upper: key,
		})
		key = info.Parent
	}
	if key == diff.Lower {
		// The diff is sliceable, return the subdiffs that should be applied.
		// We had to iterate over diffs from child to parent, so reverse them back to the correct order.
		for i := len(diffs)/2 - 1; i >= 0; i-- {
			opp := len(diffs) - 1 - i
			diffs[i], diffs[opp] = diffs[opp], diffs[i]
		}
		return diffs, nil
	}
	// We can't slice this diff into subdiffs, so just return the original one
	return []Diff{diff}, nil
}

func (sn *mergeSnapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	// If key was created by Merge, we may need to use the annotated mergeUsage key as
	// the snapshotter's usage method is wrong when hardlinks are used to create the merge.
	if info, err := sn.Stat(ctx, key); err != nil {
		return snapshots.Usage{}, err
	} else if usage, ok, err := mergeUsageOf(info); err != nil {
		return snapshots.Usage{}, err
	} else if ok {
		return usage, nil
	}
	return sn.Snapshotter.Usage(ctx, key)
}

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
