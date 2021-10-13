package snapshot

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/pkg/userns"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/continuity/sysx"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/leaseutil"
	"github.com/moby/buildkit/util/overlay"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
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
	Merge(ctx context.Context, key string, parents []string, opts ...snapshots.Opt) error

	// TODO: Doc
	Diff(ctx context.Context, key, lower, upper string, opts ...snapshots.Opt) error
}

type mergeSnapshotter struct {
	Snapshotter
	lm leases.Manager

	// Whether we should try to implement merges by hardlinking between underlying directories
	useHardlinks bool

	// Whether we can re-use the first merge input or have to create all merges from scratch.
	// On overlay-based snapshotters, we can use the base merge input as a parent w/out
	// needing to copy it.
	reuseBaseMergeInput bool

	// TODO:
	useOverlaySliceDiff bool
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
		Snapshotter:         sn,
		lm:                  lm,
		useHardlinks:        useHardlinks,
		reuseBaseMergeInput: overlayBased,
		useOverlaySliceDiff: overlayBased,
	}
}

func (sn *mergeSnapshotter) Diff(ctx context.Context, key, lower, upper string, opts ...snapshots.Opt) error {
	// check to see if the diff can be represented by slicing lowerdirs
	if lower, upper, err := sn.asDiffSlice(ctx, lower, upper); err != nil {
		return err
	} else if lower != "" && upper != "" {
		prepareKey := identity.NewID()
		if err := sn.Prepare(ctx, prepareKey, ""); err != nil {
			return errors.Wrapf(err, "failed to prepare %q", key)
		}
		if err := sn.Commit(ctx, key, prepareKey, withDiffSnapshotKeys(lower, upper)); err != nil {
			return errors.Wrap(err, "failed to commit optimized diff snapshot")
		}
		return nil
	}

	prepareKey := identity.NewID()
	if err := sn.Prepare(ctx, prepareKey, ""); err != nil {
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

	lowerMountable, err := sn.View(tempLeaseCtx, identity.NewID(), lower)
	if err != nil {
		return errors.Wrap(err, "failed to create view for lower key of diff")
	}

	upperMountable, err := sn.View(tempLeaseCtx, identity.NewID(), upper)
	if err != nil {
		return errors.Wrap(err, "failed to create view for upper key of diff")
	}

	// externalHardlinks keeps track of which inodes have been hard-linked between snapshots (which is
	//enabled when sn.useHardlinks is set to true)
	externalHardlinks := make(map[uint64]struct{})

	if err := diffApply(ctx, lowerMountable, upperMountable, applyMountable, sn.useHardlinks, externalHardlinks); err != nil {
		return errors.Wrapf(err, "failed to create diff of lower %q and upper %q", lower, upper)
	}

	// save the correctly calculated usage as a label on the committed key
	usage, err := diskUsage(ctx, applyMountable, externalHardlinks)
	if err != nil {
		return errors.Wrap(err, "failed to get disk usage of diff apply")
	}
	if err := sn.Commit(ctx, key, prepareKey, withUsage(usage)); err != nil {
		return errors.Wrapf(err, "failed to commit %q", key)
	}
	return nil
}

func (sn *mergeSnapshotter) slice(ctx context.Context, key string) ([]snapshots.Info, error) {
	var lower string
	info, err := sn.Stat(ctx, key)
	if err != nil {
		return nil, err
	}
	if diffLower, diffUpper := diffSnapshotKeysOf(info); diffLower != "" && diffUpper != "" {
		lower = diffLower
		info, err = sn.Stat(ctx, diffUpper)
		if err != nil {
			return nil, err
		}
	}

	infos := []snapshots.Info{info}
	key = info.Parent
	for key != "" && key != lower {
		info, err := sn.Stat(ctx, key)
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
		key = info.Parent
	}
	for i, j := 0, len(infos)-1; i < j; i, j = i+1, j-1 {
		infos[i], infos[j] = infos[j], infos[i]
	}
	return infos, nil
}

func (sn *mergeSnapshotter) asDiffSlice(ctx context.Context, lower, upper string) (string, string, error) {
	if lower == "" {
		return "", "", nil // TODO: kind of can be a slice sorta..
	}
	if upper == "" {
		return "", "", nil
	}

	lowerSlice, err := sn.slice(ctx, lower)
	if err != nil {
		return "", "", err
	}
	upperSlice, err := sn.slice(ctx, upper)
	if err != nil {
		return "", "", err
	}

	if len(lowerSlice) >= len(upperSlice) {
		return "", "", nil
	}

	if upperSlice[len(lowerSlice)-1].Name != lowerSlice[len(lowerSlice)-1].Name {
		return "", "", nil
	}
	return upperSlice[len(lowerSlice)-1].Name, upperSlice[len(upperSlice)-1].Name, nil
}

func (sn *mergeSnapshotter) Merge(ctx context.Context, key string, parents []string, opts ...snapshots.Opt) error {
	// Flatten the provided parent keys out. If any of them are them selves merges, don't use that key directly as
	// a parent and instead use each of the merge inputs to that parent directly in the merge here. This is important
	// for ensuring deletions defined between layers are preserved as part of each merge.
	var mergeKeys []string
	for _, parentKey := range parents {
		parentInfo, err := sn.Stat(ctx, parentKey)
		if err != nil {
			return errors.Wrap(err, "failed to stat parent during merge")
		}
		if parentInfo.Kind != snapshots.KindCommitted {
			return errors.Wrapf(err, "invalid kind %q for parent key %q provided to merge", parentInfo.Kind, parentKey)
		}
		if parentKeys := mergeKeysOf(parentInfo); parentKeys != nil {
			mergeKeys = append(mergeKeys, parentKeys...)
		} else {
			mergeKeys = append(mergeKeys, parentKey)
		}
	}

	// Map each key in mergeKeys to each key making up the snapshot chain.
	// applyChains' outer slice has the same order as parents, which is expected to be lowest merge input to highest.
	// applyChains' inner slice is ordered from highest layer to lowest.
	var applyChains [][]string
	var baseKey string
	for i, parent := range mergeKeys {
		if i == 0 && sn.reuseBaseMergeInput {
			// Overlay-based snapshotters can skip the base of the merge and just use it as the parent of the merge snapshot.
			// Other snapshotters will start empty (with baseKey set to "")
			baseKey = parent
			continue
		}
		var chain []string
		for curkey := parent; curkey != ""; {
			info, err := sn.Stat(ctx, curkey)
			if err != nil {
				return errors.Wrapf(err, "failed to stat chain key %q", curkey)
			}
			chain = append(chain, info.Name)
			curkey = info.Parent
		}
		// add an empty key as the bottom layer so that the real bottom layer gets diffed with it and applied
		chain = append(chain, "")
		applyChains = append(applyChains, chain)
	}

	// Make the snapshot that will be merged into
	prepareKey := identity.NewID()
	if err := sn.Prepare(ctx, prepareKey, baseKey); err != nil {
		return errors.Wrapf(err, "failed to prepare %q", key)
	}
	applyMounts, err := sn.Mounts(ctx, prepareKey)
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
	for _, chain := range applyChains {
		for j := range chain[:len(chain)-1] {
			lower := chain[len(chain)-1-j]
			upper := chain[len(chain)-2-j]

			var lowerMounts Mountable
			if lower != "" {
				viewID := identity.NewID()
				var err error
				lowerMounts, err = sn.View(tempLeaseCtx, viewID, lower)
				if err != nil {
					return errors.Wrapf(err, "failed to get mounts of lower %q", lower)
				}
			}

			viewID := identity.NewID()
			upperMounts, err := sn.View(tempLeaseCtx, viewID, upper)
			if err != nil {
				return errors.Wrapf(err, "failed to get mounts of upper %q", upper)
			}

			err = diffApply(tempLeaseCtx, lowerMounts, upperMounts, applyMounts, sn.useHardlinks, externalHardlinks)
			if err != nil {
				return err
			}
		}
	}

	// save the correctly calculated usage as a label on the committed key
	usage, err := diskUsage(ctx, applyMounts, externalHardlinks)
	if err != nil {
		return errors.Wrap(err, "failed to get disk usage of diff apply merge")
	}
	if err := sn.Commit(ctx, key, prepareKey, withUsage(usage), withMergeKeys(mergeKeys)); err != nil {
		return errors.Wrapf(err, "failed to commit %q", key)
	}
	return nil
}

func (sn *mergeSnapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) error {
	if parent != "" {
		if info, err := sn.Stat(ctx, parent); err != nil {
			return err
		} else if lower, upper := diffSnapshotKeysOf(info); lower != "" && upper != "" {
			opts = append(opts, withDiffSnapshotKeys(lower, key))
		}
	}
	return sn.Snapshotter.Prepare(ctx, key, parent, opts...)
}

func (sn *mergeSnapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	info, err := sn.Stat(ctx, key)
	if err != nil {
		return err
	}
	if lower, upper := diffSnapshotKeysOf(info); lower != "" && upper != "" {
		opts = append(opts, withDiffSnapshotKeys(lower, name))
	}

	// TODO: doc, this fixes the corner case w/ overlay snapshotters where a file created and deleted results in
	// TODO: copied up directories that are not actually part of the diff this layer represents.
	// TODO: Be sure to note that this work is fairly cheap and also amortizes work that would be done later by exporter
	// TODO: We will need to apply this fix for already existing snapshots in the cache too...
	// TODO: Need to handle what happens if there is a crash in the middle of this, make sure state is always consistent
	mountable, err := sn.Mounts(ctx, key)
	if err != nil {
		return err
	}
	mnts, unmount, err := mountable.Mount()
	if err != nil {
		return err
	}
	defer unmount()
	if len(mnts) == 1 && mnts[0].Type == "overlay" {
		mnt := mnts[0]
		var upperdir string
		for _, opt := range mnt.Options {
			if strings.HasPrefix(opt, "upperdir=") {
				upperdir = strings.SplitN(opt, "=", 2)[1]
				break
			}
		}
		if upperdir != "" {
			leafDirs := make(map[string]struct{})
			filepath.WalkDir(upperdir, func(path string, d fs.DirEntry, prevErr error) error {
				if prevErr != nil {
					return prevErr
				}

				// skip root
				if path == upperdir {
					return nil
				}

				delete(leafDirs, filepath.Dir(path))
				if d.IsDir() {
					xattrs, err := sysx.LListxattr(path)
					if err != nil {
						return err
					}
					for _, xattr := range xattrs {
						if isOpaqueXattr(xattr) {
							return fs.SkipDir
						}
					}
					leafDirs[path] = struct{}{}
				}
				return nil
			})
			if len(leafDirs) > 0 {
				var parentMntOptions []string
				var lowerdirs []string
				for _, opt := range mnt.Options {
					if strings.HasPrefix(opt, "upperdir=") || strings.HasPrefix(opt, "workdir=") {
						continue
					}
					if strings.HasPrefix(opt, "lowerdir=") {
						lowerdirs = strings.Split(strings.SplitN(opt, "=", 2)[1], ":")
					}
					parentMntOptions = append(parentMntOptions, opt)
				}
				var parentMnts []mount.Mount
				if len(lowerdirs) == 0 {
					return errors.New("invalid overlay mount with no lowerdirs")
				}
				if len(lowerdirs) == 1 {
					parentMnts = []mount.Mount{{
						Type:    "bind",
						Source:  lowerdirs[0],
						Options: []string{"rbind", "ro"},
					}}
				} else {
					parentMnts = []mount.Mount{{
						Type:    "overlay",
						Source:  "overlay",
						Options: parentMntOptions,
					}}
				}

				if err := withTempMount(ctx, parentMnts, func(parentMntRoot string, isDirect bool) error {
					for len(leafDirs) > 0 {
						newLeafDirs := make(map[string]struct{})
						for leafDir := range leafDirs {
							relpath, err := filepath.Rel(upperdir, leafDir)
							if err != nil {
								return err
							}
							parentPath := filepath.Join(parentMntRoot, relpath)

							parentStat, err := os.Lstat(parentPath)
							if os.IsNotExist(err) || errors.Is(err, unix.ENOTDIR) {
								// this is a real diff because the directory didn't exist before, keep it
								continue
							}
							if err != nil {
								return err
							}

							leafStat, err := os.Lstat(leafDir)
							if err != nil {
								return err
							}

							if same, err := overlay.SameDir(leafStat, parentStat, leafDir, parentPath); err != nil {
								return err
							} else if same {
								if err := os.Remove(leafDir); err != nil {
									return err
								}
								checkLeafDir := filepath.Dir(leafDir)
								if checkLeafDir == upperdir {
									continue
								}
								if dirents, err := os.ReadDir(checkLeafDir); err != nil {
									return err
								} else if len(dirents) == 0 {
									newLeafDirs[checkLeafDir] = struct{}{}
								}
							}
						}
						leafDirs = newLeafDirs
					}
					return nil
				}); err != nil {
					return err
				}
			}
		}
	}

	return sn.Snapshotter.Commit(ctx, name, key, opts...)
}

func (sn *mergeSnapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) (Mountable, error) {
	if parent != "" {
		if info, err := sn.Stat(ctx, parent); err != nil {
			return nil, err
		} else if lower, upper := diffSnapshotKeysOf(info); lower != "" && upper != "" {
			opts = append(opts, withDiffSnapshotKeys(lower, upper))
		}
	}
	return sn.Snapshotter.View(ctx, key, parent, opts...)
}

func (sn *mergeSnapshotter) Mounts(ctx context.Context, key string) (Mountable, error) {
	// TODO: comments
	if info, err := sn.Stat(ctx, key); err != nil {
		return nil, err
	} else if lower, upper := diffSnapshotKeysOf(info); lower != "" && upper != "" {
		// TODO: temp leases on views
		lowerView := identity.NewID()
		lowerMountable, err := sn.Snapshotter.View(ctx, lowerView, lower)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get lower view for %s", key)
		}
		var lowerLayers []string
		mounts, unmountLower, err := lowerMountable.Mount()
		if err != nil {
			return nil, err
		}
		defer unmountLower()
		if len(mounts) == 1 {
			mnt := mounts[0]
			switch mnt.Type {
			case "bind", "rbind":
				lowerLayers = []string{mnt.Source}
			case "overlay":
				layers, err := overlay.GetOverlayLayers(mnt)
				if err != nil {
					return nil, err
				}
				lowerLayers = layers
			default:
				// TODO: just fallback to normal diff in this case, not error
				return nil, errors.Errorf("invalid mount type %q for snapshot %q", mnt.Type, lower)
			}
		} else {
			return nil, errors.Errorf("invalid number of mounts %d for snapshot %q", len(mounts), lower)
		}

		var upperMountable Mountable
		if upper == key {
			// case where key is an active snapshot
			// TODO: this condition is confusing
			upperMountable, err = sn.Snapshotter.Mounts(ctx, upper)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to get upper mounts for %s", key)
			}
		} else {
			// TODO: temp leases on views
			upperView := identity.NewID()
			upperMountable, err = sn.Snapshotter.View(ctx, upperView, upper)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to get upper view for %s", key)
			}
		}
		var upperLayers []string
		mounts, unmountUpper, err := upperMountable.Mount()
		if err != nil {
			return nil, err
		}
		defer unmountUpper()
		if len(mounts) == 1 {
			mnt := mounts[0]
			switch mnt.Type {
			case "bind", "rbind":
				upperLayers = []string{mnt.Source}
			case "overlay":
				layers, err := overlay.GetOverlayLayers(mnt)
				if err != nil {
					return nil, err
				}
				upperLayers = layers
			default:
				// TODO: just fallback to normal diff in this case, not error
				return nil, errors.Errorf("invalid mount type %q for snapshot %q", mnt.Type, lower)
			}
		} else {
			return nil, errors.Errorf("invalid number of mounts %d for snapshot %q", len(mounts), lower)
		}

		slicedLayers := upperLayers[len(lowerLayers):]
		if len(slicedLayers) == 0 {
			return nil, errors.Errorf("invalid empty diff for snapshot %q", key)
		}
		if len(slicedLayers) == 1 {
			// TODO: doc, this deals with corner case where a bind might might directly reveal whiteout devices
			// TODO: Using an empty view is nice in that cleanup is handled in all cases automatically
			// TODO: temp lease on view
			// TODO: Make sure this doesn't confuse the overlay differ on export
			tempViewID := identity.NewID()
			mntable, err := sn.View(ctx, tempViewID, "")
			if err != nil {
				return nil, err
			}
			mnts, unmount, err := mntable.Mount()
			if err != nil {
				return nil, err
			}
			defer unmount()
			if len(mnts) != 1 {
				return nil, errors.New("invalid multiple mounts")
			}
			mnt := mnts[0]
			// TODO: more validation this is a bind? Should always be, but still
			emptyDir := mnt.Source
			slicedLayers = []string{emptyDir, slicedLayers[0]}
		}

		for i, j := 0, len(slicedLayers)-1; i < j; i, j = i+1, j-1 {
			slicedLayers[i], slicedLayers[j] = slicedLayers[j], slicedLayers[i]
		}
		// TODO: include special options from the original overlay mount, especially userxattr
		return &staticMountable{mounts: []mount.Mount{{
			Type:    "overlay",
			Source:  "overlay",
			Options: []string{fmt.Sprintf("lowerdir=%s", strings.Join(slicedLayers, ":"))},
		}}, idmap: sn.IdentityMapping(), id: key}, nil
	}

	return sn.Snapshotter.Mounts(ctx, key)
}

func (sn *mergeSnapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	// If key was created by Merge, we may need to use the annotated mergeUsage key as
	// the snapshotter's usage method is wrong when hardlinks are used to create the merge.
	if info, err := sn.Stat(ctx, key); err != nil {
		return snapshots.Usage{}, err
	} else if usage, ok, err := usageOf(info); err != nil {
		return snapshots.Usage{}, err
	} else if ok {
		return usage, nil
	}
	return sn.Snapshotter.Usage(ctx, key)
}

// usage{Size,Inodes}Label hold the correct usage calculations for snapshots created via diffApply, for
// which the builtin usage is wrong because it can't account for hardlinks made across immutable snapshots
const usageSizeLabel = "buildkit.usageSize"
const usageInodesLabel = "buildkit.usageInodes"

func withUsage(usage snapshots.Usage) snapshots.Opt {
	return snapshots.WithLabels(map[string]string{
		usageSizeLabel:   strconv.Itoa(int(usage.Size)),
		usageInodesLabel: strconv.Itoa(int(usage.Inodes)),
	})
}

func usageOf(info snapshots.Info) (usage snapshots.Usage, ok bool, rerr error) {
	if info.Labels == nil {
		return snapshots.Usage{}, false, nil
	}
	if str, ok := info.Labels[usageSizeLabel]; ok {
		i, err := strconv.Atoi(str)
		if err != nil {
			return snapshots.Usage{}, false, err
		}
		usage.Size = int64(i)
	}
	if str, ok := info.Labels[usageInodesLabel]; ok {
		i, err := strconv.Atoi(str)
		if err != nil {
			return snapshots.Usage{}, false, err
		}
		usage.Inodes = int64(i)
	}
	return usage, true, nil
}

// mergeKeysLabel holds the keys that a given snapshot should be merged on top of in order to be mounted
const mergeKeysLabel = "containerd.io/snapshot/buildkit.mergeKeys"

func withMergeKeys(keys []string) snapshots.Opt {
	return func(info *snapshots.Info) error {
		// make sure no keys have "," in them
		for _, k := range keys {
			if strings.Contains(k, ",") {
				return errors.Errorf("invalid merge key containing \",\": %s", k)
			}
		}
		return snapshots.WithLabels(map[string]string{
			mergeKeysLabel: strings.Join(keys, ","),
		})(info)
	}
}

func mergeKeysOf(info snapshots.Info) []string {
	if v := info.Labels[mergeKeysLabel]; v != "" {
		return strings.Split(v, ",")
	}
	return nil
}

// TODO: doc
const lowerDiffSnapshotKey = "containerd.io/snapshot/buildkit.lowerDiffSnapshotKey"
const upperDiffSnapshotKey = "containerd.io/snapshot/buildkit.upperDiffSnapshotKey"

func withDiffSnapshotKeys(lower, upper string) snapshots.Opt {
	return snapshots.WithLabels(map[string]string{
		lowerDiffSnapshotKey: lower,
		upperDiffSnapshotKey: upper,
	})
}

func diffSnapshotKeysOf(info snapshots.Info) (lower string, upper string) {
	if v := info.Labels[lowerDiffSnapshotKey]; v != "" {
		lower = v
	}
	if v := info.Labels[upperDiffSnapshotKey]; v != "" {
		upper = v
	}
	return lower, upper
}
