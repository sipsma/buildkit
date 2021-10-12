//go:build !windows
// +build !windows

package snapshot

import (
	"context"
	"fmt"
	gofs "io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/continuity/fs"
	"github.com/containerd/continuity/sysx"
	"github.com/containerd/stargz-snapshotter/snapshot/overlayutils"
	"github.com/hashicorp/go-multierror"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/leaseutil"
	"github.com/moby/buildkit/util/overlay"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

func (sn *mergeSnapshotter) diffApply(ctx context.Context, dest Mountable, diffs ...Diff) (map[uint64]struct{}, error) {
	// TODO: doc that ctx is assumed to have a temp lease

	changes := make(chan *change, 16) // TODO: use a buffered channel? what size?

	var cleanups []func() error
	defer func() {
		for _, c := range cleanups {
			if c == nil {
				continue
			}
			if err := c(); err != nil {
				bklog.G(ctx).WithError(err).Error("failed to cleanup")
			}
		}
	}()

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		defer close(changes)
		for _, diff := range diffs {
			var lowerMntable Mountable
			if diff.Lower != "" {
				if info, err := sn.Stat(ctx, diff.Lower); err != nil {
					return err
				} else if info.Kind == snapshots.KindCommitted {
					lowerMntable, err = sn.View(ctx, identity.NewID(), diff.Lower)
					if err != nil {
						return err
					}
				} else {
					lowerMntable, err = sn.Mounts(ctx, diff.Lower)
					if err != nil {
						return err
					}
				}
			}
			var upperMntable Mountable
			if diff.Upper != "" {
				if info, err := sn.Stat(ctx, diff.Upper); err != nil {
					return err
				} else if info.Kind == snapshots.KindCommitted {
					upperMntable, err = sn.View(ctx, identity.NewID(), diff.Upper)
					if err != nil {
						return err
					}
				} else {
					upperMntable, err = sn.Mounts(ctx, diff.Upper)
					if err != nil {
						return err
					}
				}
			}
			if cleanup, err := sendChanges(ctx, lowerMntable, upperMntable, changes); err != nil {
				return err
			} else {
				cleanups = append(cleanups, cleanup)
			}
		}
		return nil
	})

	var externalHardlinks map[uint64]struct{}
	eg.Go(func() error {
		var err error
		externalHardlinks, err = applyChanges(ctx, dest, changes)
		return err
	})

	return externalHardlinks, eg.Wait()
}

type change struct {
	kind    fs.ChangeKind
	subpath string
	srcpath string
	srcStat *syscall.Stat_t
}

func sendChanges(ctx context.Context, lower, upper Mountable, changes chan<- *change) (func() error, error) {
	var lowerMnts []mount.Mount
	if lower != nil {
		mnts, lowerUnmount, err := lower.Mount()
		if err != nil {
			return nil, err
		}
		defer lowerUnmount()
		lowerMnts = mnts
	}

	var upperMnts []mount.Mount
	if upper != nil {
		mnts, upperUnmount, err := upper.Mount()
		if err != nil {
			return nil, err
		}
		defer upperUnmount()
		upperMnts = mnts
	}

	if upperdir, err := overlay.GetUpperdir(lowerMnts, upperMnts); err == nil {
		return nil, overlayDiff(ctx, lowerMnts, upperMnts, upperdir, changes)
	}

	return walkingDiff(ctx, lowerMnts, upperMnts, changes)
}

func walkingDiff(ctx context.Context, lowerMnts, upperMnts []mount.Mount, changes chan<- *change) (cleanup func() error, rerr error) {
	lowerMounter := LocalMounterWithMounts(lowerMnts)
	lower, err := lowerMounter.Mount()
	if err != nil {
		return nil, err
	}
	upperMounter := LocalMounterWithMounts(upperMnts)
	upper, err := upperMounter.Mount()
	if err != nil {
		lowerMounter.Unmount()
		return nil, err
	}

	cleanup = func() error {
		return multierror.Append(lowerMounter.Unmount(), upperMounter.Unmount()).ErrorOrNil()
	}
	defer func() {
		if rerr != nil {
			cleanup()
			cleanup = nil
		}
	}()

	visited := make(map[string]struct{})

	return cleanup, fs.Changes(ctx, lower, upper, func(kind fs.ChangeKind, changePath string, srcfi os.FileInfo, prevErr error) error {
		if prevErr != nil {
			return prevErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		srcpath, err := safeJoin(upper, changePath)
		if err != nil {
			return err
		}

		c := &change{
			kind:    kind,
			subpath: changePath,
			srcpath: srcpath,
		}

		// Add any parent directories that haven't been visited, even when they aren't modified according to
		// the double-walking differ. This is important because it matches the behavior of containerd's archive
		// diff writer, which we want to be consistent with in order to ensure that merge+diff refs are equivalent.
		// before and after exporting/importing.
		//
		// Examples of cases where this makes a difference include when the diff is being made to a file /foo/bar
		// but in the destination being applied to /foo is a symlink, not a directory. In this case, if we don't
		// include a change for /foo, it will remain a symlink and be followed when being applied to.
		var checkParent func(string) error
		checkParent = func(subpath string) error {
			if subpath == "/" {
				return nil
			}
			if err := checkParent(filepath.Dir(subpath)); err != nil {
				return err
			}
			srcpath, err := safeJoin(upper, subpath)
			if err != nil {
				return err
			}
			if _, ok := visited[srcpath]; ok {
				return nil
			}
			visited[srcpath] = struct{}{}
			srcfi, err := os.Lstat(srcpath)
			if err != nil {
				return err
			}
			srcStat, ok := srcfi.Sys().(*syscall.Stat_t)
			if !ok {
				return errors.Errorf("unexpected type %T", srcfi)
			}
			select {
			case changes <- &change{
				kind:    fs.ChangeKindModify,
				subpath: subpath,
				srcpath: srcpath,
				srcStat: srcStat,
			}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := checkParent(filepath.Dir(c.subpath)); err != nil {
			return err
		}
		visited[c.srcpath] = struct{}{}

		// Try to ensure that srcpath and srcStat are set to a file from the underlying filesystem
		// rather than the actual mount when possible. This allows hardlinking without getting EXDEV.
		if len(upperMnts) == 1 && srcfi != nil && !srcfi.IsDir() {
			mnt := upperMnts[0]
			switch mnt.Type {
			case "bind", "rbind":
				srcpath, err := safeJoin(mnt.Source, c.subpath)
				if err != nil {
					return err
				}
				c.srcpath = srcpath
				if fi, err := os.Lstat(c.srcpath); err == nil {
					srcfi = fi
				} else {
					return errors.Wrap(err, "failed to stat underlying file from bind mount")
				}
			case "overlay":
				overlayDirs, err := overlay.GetOverlayLayers(mnt)
				if err != nil {
					return errors.Wrapf(err, "failed to get overlay layers from mount %+v", mnt)
				}
				for i := range overlayDirs {
					dir := overlayDirs[len(overlayDirs)-1-i]
					path, err := safeJoin(dir, c.subpath)
					if err != nil {
						return err
					}
					if stat, err := os.Lstat(path); err == nil {
						c.srcpath = path
						srcfi = stat
						break
					} else if errors.Is(err, unix.ENOENT) {
						continue
					} else {
						return errors.Wrap(err, "failed to lstat when finding direct path of overlay file")
					}
				}
			}
		}

		if srcfi != nil {
			var ok bool
			c.srcStat, ok = srcfi.Sys().(*syscall.Stat_t)
			if !ok {
				return errors.Errorf("unhandled stat type for %+v", srcfi)
			}
		}

		select {
		case changes <- c:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
}

func overlayDiff(ctx context.Context, lowerMnts, upperMnts []mount.Mount, upperdir string, changes chan<- *change) error {
	lowerMounter := LocalMounterWithMounts(lowerMnts)
	lower, err := lowerMounter.Mount()
	if err != nil {
		return err
	}
	defer lowerMounter.Unmount()
	upperMounter := LocalMounterWithMounts(upperMnts)
	upper, err := upperMounter.Mount()
	if err != nil {
		return err
	}
	defer upperMounter.Unmount()

	// TODO:(sipsma) it's possible to skip checking file contents in some cases, namely where a change
	// has already been applied to the given path.
	checkFileContents := true

	visited := make(map[string]struct{})

	return overlay.Changes(ctx, func(kind fs.ChangeKind, changePath string, srcfi os.FileInfo, prevErr error) error {
		if prevErr != nil {
			return prevErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		srcpath, err := safeJoin(upperdir, changePath)
		if err != nil {
			return err
		}

		c := &change{
			kind:    kind,
			subpath: changePath,
			srcpath: srcpath,
		}

		// TODO: dedupe
		var checkParent func(string) error
		checkParent = func(subpath string) error {
			if subpath == "/" {
				return nil
			}
			if err := checkParent(filepath.Dir(subpath)); err != nil {
				return err
			}
			srcpath, err := safeJoin(upper, subpath)
			if err != nil {
				return err
			}
			if _, ok := visited[srcpath]; ok {
				return nil
			}
			visited[srcpath] = struct{}{}
			srcfi, err := os.Lstat(srcpath)
			if err != nil {
				return err
			}
			srcStat, ok := srcfi.Sys().(*syscall.Stat_t)
			if !ok {
				return errors.Errorf("unexpected type %T", srcfi)
			}
			select {
			case changes <- &change{
				kind:    fs.ChangeKindModify,
				subpath: subpath,
				srcpath: srcpath,
				srcStat: srcStat,
			}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := checkParent(filepath.Dir(c.subpath)); err != nil {
			return err
		}
		visited[c.srcpath] = struct{}{}

		if srcfi != nil {
			var ok bool
			c.srcStat, ok = srcfi.Sys().(*syscall.Stat_t)
			if !ok {
				return errors.Errorf("unhandled stat type for %+v", srcfi)
			}
		}

		select {
		case changes <- c:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, upperdir, upper, lower, checkFileContents)
}

func applyChanges(ctx context.Context, dest Mountable, changes <-chan *change) (map[uint64]struct{}, error) {
	mnts, unmount, err := dest.Mount()
	if err != nil {
		return nil, nil
	}
	defer unmount()

	if len(mnts) != 1 {
		return nil, errors.Errorf("expected exactly one mount, got %d", len(mnts))
	}
	mnt := mnts[0]

	app := &applier{
		visited:           make(map[string]struct{}),
		inodes:            make(map[uint64]string),
		externalHardlinks: make(map[uint64]struct{}),
		times:             make(map[string]*pathTime),
	}

	switch mnt.Type {
	case "overlay":
		for _, opt := range mnt.Options {
			if strings.HasPrefix(opt, "upperdir=") {
				app.root = strings.TrimPrefix(opt, "upperdir=")
			}
		}
		if app.root == "" {
			return nil, errors.Errorf("could not find upperdir in mount options %v", mnt.Options)
		}
		app.whiteoutDeletes = true
		app.tryHardlink = true
	case "bind", "rbind":
		app.root = mnt.Source
		app.tryHardlink = true
	default:
		mounter := LocalMounter(dest)
		root, err := mounter.Mount()
		if err != nil {
			return nil, err
		}
		defer mounter.Unmount()
		app.root = root
	}

	for c := range changes {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if c == nil {
			return nil, errors.New("nil change")
		}

		if c.kind == fs.ChangeKindUnmodified {
			continue
		}

		app.visited[c.srcpath] = struct{}{}

		dstpath, err := safeJoin(app.root, c.subpath)
		if err != nil {
			return nil, err
		}

		// TODO:
		// TODO:
		// TODO:
		// TODO:
		// TODO:
		// TODO:
		bklog.G(ctx).Debugf("applyChanges: %q %q %q", c.kind, c.srcpath, dstpath)

		tryHardlink := app.tryHardlink
		if c.kind == fs.ChangeKindDelete {
			if app.whiteoutDeletes {
				c.kind = fs.ChangeKindModify
				if c.srcStat == nil {
					// we don't have a whiteout to link from, we will have to mknod one
					tryHardlink = false
					c.srcStat = &syscall.Stat_t{
						Mode: syscall.S_IFCHR,
						Rdev: unix.Mkdev(0, 0),
						// TODO: do we care about times?
					}
				}
			} else {
				if err := os.RemoveAll(dstpath); err != nil {
					return nil, errors.Wrap(err, "failed to remove during apply")
				}
				continue
			}
		}

		if err := app.addOrModify(ctx, c.subpath, c.srcpath, c.srcStat, dstpath, tryHardlink); err != nil {
			return nil, err
		}
	}

	// Set times now that everything has been modified. Set them from lowest to highest
	// to deal with the fact that changing the times of a file will change the times of
	// its parent directory.
	var times []*pathTime
	for _, ts := range app.times {
		times = append(times, ts)
	}
	sort.Slice(times, func(i, j int) bool {
		return times[i].path > times[j].path
	})
	for _, ts := range app.times {
		if err := ts.apply(); err != nil {
			return nil, err
		}
	}

	return app.externalHardlinks, nil
}

type applier struct {
	root              string
	tryHardlink       bool
	whiteoutDeletes   bool
	visited           map[string]struct{}
	inodes            map[uint64]string
	externalHardlinks map[uint64]struct{}
	times             map[string]*pathTime
}

type pathTime struct {
	path  string
	atime unix.Timespec
	mtime unix.Timespec
}

func (pt *pathTime) apply() error {
	err := unix.UtimesNanoAt(unix.AT_FDCWD, pt.path, []unix.Timespec{pt.atime, pt.mtime}, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENOTDIR) {
		return nil
	}
	return err
}

// TODO: better name?
func (app *applier) addOrModify(ctx context.Context, subpath string, srcpath string, srcStat *syscall.Stat_t, dstpath string, tryHardlink bool) error {
	// if there is an existing file/dir at the dstpath, delete it unless both it and srcpath are dirs (in which case they get merged)
	var dstStat *syscall.Stat_t
	if dstfi, err := os.Lstat(dstpath); err == nil {
		stat, ok := dstfi.Sys().(*syscall.Stat_t)
		if !ok {
			return errors.Errorf("failed to get stat_t for %T", dstStat)
		}
		dstStat = stat
	} else if !os.IsNotExist(err) {
		return errors.Wrap(err, "failed to stat during copy apply")
	}
	if dstStat != nil && srcStat.Mode&dstStat.Mode&unix.S_IFMT != unix.S_IFDIR {
		if err := os.RemoveAll(dstpath); err != nil {
			return errors.Wrap(err, "failed to remove existing path during copy apply")
		}
		dstStat = nil
	}

	if tryHardlink {
		if itWorked, err := app.hardlinkFile(ctx, srcpath, srcStat, dstpath); err != nil {
			return err
		} else if itWorked {
			return nil
		}
	}

	if err := app.copyFile(ctx, srcpath, srcStat, dstpath, dstStat); err != nil {
		return err
	}
	return nil
}

func (app *applier) hardlinkFile(ctx context.Context, srcpath string, srcStat *syscall.Stat_t, dstpath string) (bool, error) {
	switch srcStat.Mode & unix.S_IFMT {
	case unix.S_IFDIR, unix.S_IFIFO, unix.S_IFSOCK:
		// Directories can't be hard-linked, so they just have to be recreated.
		// Named pipes and sockets can be hard-linked but is best to avoid as it could enable IPC in weird cases.
		return false, nil

	default:
		if err := os.Link(srcpath, dstpath); errors.Is(err, unix.EXDEV) || errors.Is(err, unix.EMLINK) {
			// These errors are expected when the hardlink would cross devices or would exceed the maximum number of links for the inode.
			// Just fallback to a copy.
			// TODO:
			// TODO:
			// TODO:
			// TODO:
			// TODO:
			// TODO:
			// TODO: Leave this as a debug log?
			fmt.Printf("%s -> %s %v\n", srcpath, dstpath, err)

			return false, nil
		} else if err != nil {
			return false, errors.Wrap(err, "failed to hardlink during apply")
		}

		// mark this inode as one coming from a separate snapshot, needed for disk usage calculations elsewhere
		app.externalHardlinks[srcStat.Ino] = struct{}{}
		return true, nil
	}
}

func (app *applier) copyFile(ctx context.Context, srcpath string, srcStat *syscall.Stat_t, dstpath string, dstStat *syscall.Stat_t) error {
	switch srcStat.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		if srcStat.Nlink > 1 {
			if linkedPath, ok := app.inodes[srcStat.Ino]; ok {
				if err := os.Link(linkedPath, dstpath); err != nil {
					return errors.Wrap(err, "failed to create hardlink during copy file apply")
				}
				return nil // no other metadata updates needed when hardlinking
			}
			app.inodes[srcStat.Ino] = dstpath
		}
		if err := fs.CopyFile(dstpath, srcpath); err != nil {
			return errors.Wrapf(err, "failed to copy from %s to %s during apply", srcpath, dstpath)
		}
	case unix.S_IFDIR:
		if dstStat == nil {
			// dstpath doesn't exist, make it a dir
			if err := unix.Mkdir(dstpath, srcStat.Mode); err != nil {
				return errors.Wrapf(err, "failed to create applied dir at %q from %q", dstpath, srcpath)
			}
		}
	case unix.S_IFLNK:
		if target, err := os.Readlink(srcpath); err != nil {
			return errors.Wrap(err, "failed to read symlink during apply")
		} else if err := os.Symlink(target, dstpath); err != nil {
			return errors.Wrap(err, "failed to create symlink during apply")
		}
	case unix.S_IFBLK, unix.S_IFCHR, unix.S_IFIFO, unix.S_IFSOCK:
		if err := unix.Mknod(dstpath, srcStat.Mode, int(srcStat.Rdev)); err != nil {
			return errors.Wrap(err, "failed to create device during apply")
		}
	default:
		// should never be here, all types should be handled
		return errors.Errorf("unhandled file type %d during merge at path %q", srcStat.Mode&unix.S_IFMT, srcpath)
	}

	xattrs, err := sysx.LListxattr(srcpath)
	// ENOENT can be hit when srcpath doesn't exist, such as when we are creating a new whiteout device
	// to represent a delete on overlay.
	if err != nil && !errors.Is(err, unix.ENOENT) {
		return errors.Wrapf(err, "failed to list xattrs of src path %s", srcpath)
	}
	for _, xattr := range xattrs {
		if isOpaqueXattr(xattr) {
			// Don't recreate opaque xattrs during merge. These should only be set when using overlay snapshotters,
			// in which case we are converting from the "opaque whiteout" format to the "explicit whiteout" format during
			// the merge (as taken care of by the overlay differ).
			continue
		}
		xattrVal, err := sysx.LGetxattr(srcpath, xattr)
		if err != nil {
			return errors.Wrapf(err, "failed to get xattr %s of src path %s", xattr, srcpath)
		}
		if err := sysx.LSetxattr(dstpath, xattr, xattrVal, 0); err != nil {
			// This can often fail, so just log it: https://github.com/moby/buildkit/issues/1189
			bklog.G(ctx).Debugf("failed to set xattr %s of path %s during apply", xattr, dstpath)
		}
	}

	if err := os.Lchown(dstpath, int(srcStat.Uid), int(srcStat.Gid)); err != nil {
		return errors.Wrap(err, "failed to chown during apply")
	}

	if srcStat.Mode&unix.S_IFMT != unix.S_IFLNK {
		if err := unix.Chmod(dstpath, srcStat.Mode); err != nil {
			return errors.Wrapf(err, "failed to chmod path %q during apply", dstpath)
		}
	}

	// save the times we should set on this path, to be applied at the end.
	app.times[dstpath] = &pathTime{
		path: dstpath,
		atime: unix.Timespec{
			Sec:  srcStat.Atim.Sec,
			Nsec: srcStat.Atim.Nsec,
		},
		mtime: unix.Timespec{
			Sec:  srcStat.Mtim.Sec,
			Nsec: srcStat.Mtim.Nsec,
		},
	}

	return nil
}

func safeJoin(root, path string) (string, error) {
	dir, base := filepath.Split(path)
	parent, err := fs.RootPath(root, dir)
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, base), nil
}

// diskUsage calculates the disk space used by the provided mounts, similar to the normal containerd snapshotter disk usage
// calculations but with the extra ability to take into account hardlinks that were created between snapshots, ensuring that
// they don't get double counted.
func diskUsage(ctx context.Context, mountable Mountable, externalHardlinks map[uint64]struct{}) (snapshots.Usage, error) {
	mounts, unmount, err := mountable.Mount()
	if err != nil {
		return snapshots.Usage{}, err
	}
	defer unmount()

	inodes := make(map[uint64]struct{})
	var usage snapshots.Usage
	if err := withRWDirMount(ctx, mounts, func(root string, _ bool) error {
		return filepath.WalkDir(root, func(path string, dirent gofs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			info, err := dirent.Info()
			if err != nil {
				return err
			}
			stat := info.Sys().(*syscall.Stat_t)
			if _, ok := inodes[stat.Ino]; ok {
				return nil
			}
			inodes[stat.Ino] = struct{}{}
			if _, ok := externalHardlinks[stat.Ino]; !ok {
				usage.Inodes++
				usage.Size += stat.Blocks * 512 // 512 is always block size, see "man 2 stat"
			}
			return nil
		})
	}); err != nil {
		return snapshots.Usage{}, err
	}
	return usage, nil
}

func isOpaqueXattr(s string) bool {
	for _, k := range []string{"trusted.overlay.opaque", "user.overlay.opaque"} {
		if s == k {
			return true
		}
	}
	return false
}

// needsUserXAttr checks whether overlay mounts should be provided the userxattr option. We can't use
// NeedsUserXAttr from the overlayutils package directly because we don't always have direct knowledge
// of the root of the snapshotter state (such as when using a remote snapshotter). Instead, we create
// a temporary new snapshot and test using its root, which works because single layer snapshots will
// use bind-mounts even when created by an overlay based snapshotter.
func needsUserXAttr(ctx context.Context, sn Snapshotter, lm leases.Manager) (bool, error) {
	key := identity.NewID()

	ctx, done, err := leaseutil.WithLease(ctx, lm, leaseutil.MakeTemporary)
	if err != nil {
		return false, errors.Wrap(err, "failed to create lease for checking user xattr")
	}
	defer done(context.TODO())

	err = sn.Prepare(ctx, key, "")
	if err != nil {
		return false, err
	}
	mntable, err := sn.Mounts(ctx, key)
	if err != nil {
		return false, err
	}
	mnts, unmount, err := mntable.Mount()
	if err != nil {
		return false, err
	}
	defer unmount()

	var userxattr bool
	if err := mount.WithTempMount(ctx, mnts, func(root string) error {
		var err error
		userxattr, err = overlayutils.NeedsUserXAttr(root)
		return err
	}); err != nil {
		return false, err
	}
	return userxattr, nil
}
