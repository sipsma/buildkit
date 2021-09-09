//go:build linux
// +build linux

package snapshot

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"syscall"

	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/overlay/overlayutils"
	"github.com/containerd/continuity/devices"
	"github.com/containerd/continuity/fs"
	"github.com/containerd/continuity/sysx"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/util/leaseutil"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

const userOpaqueXattr = "user.overlay.opaque"
const trustedOpaqueXattr = "trusted.overlay.opaque"

// needsUserXAttr checks whether overlay mounts should be provided the userxattr option. We can't use
// NeedsUserXAttr from the overlayutils package directly because we don't always have direct knowledge
// of the root of the snapshotter state (such as when using a remote snapshotter). Instead, we create
// a temporary new snapshot and test using its root, which works because single layer snapshots will
// use bind-mounts even when created by an overlay based snapshotter.
func needsUserXAttr(ctx context.Context, sn snapshots.Snapshotter, lm leases.Manager) (bool, error) {
	key := identity.NewID()

	ctx, done, err := leaseutil.WithLease(ctx, lm, leaseutil.MakeTemporary)
	if err != nil {
		return false, errors.Wrap(err, "failed to create lease for checking user xattr")
	}
	defer done(context.TODO())

	mnts, err := sn.Prepare(ctx, key, "")
	if err != nil {
		return false, err
	}

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

// TODO:
// TODO:
// TODO:
// TODO:
// TODO:
// TODO:
// TODO:
// TODO:
// TODO: all of the below is just copy pasted from the cache package to avoid an import loop, should obviously be deduped in a nicer way
// There's also some small changes, namely to make sure that opaque dirs are first deleted and then recreated

// overlayChanges is continuty's `fs.Change`-like method but leverages overlayfs's
// "upperdir" for computing the diff. "upperdirView" is overlayfs mounted view of
// the upperdir that doesn't contain whiteouts. This is used for computing
// changes under opaque directories.
func overlayChanges(ctx context.Context, changeFn fs.ChangeFunc, upperdir, upperdirView, base string) error {
	return filepath.Walk(upperdir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Rebase path
		path, err = filepath.Rel(upperdir, path)
		if err != nil {
			return err
		}
		path = filepath.Join(string(os.PathSeparator), path)

		// Skip root
		if path == string(os.PathSeparator) {
			return nil
		}

		// Check if this is a deleted entry
		isDelete, skip, err := checkDelete(upperdir, path, base, f)
		if err != nil {
			return err
		} else if skip {
			return nil
		}

		var kind fs.ChangeKind
		var skipRecord bool
		var isOpaque bool
		if isDelete {
			// This is a deleted entry.
			kind = fs.ChangeKindDelete
			f = nil
		} else if baseF, err := os.Lstat(filepath.Join(base, path)); err == nil {
			// File exists in the base layer. Thus this is modified.
			kind = fs.ChangeKindModify
			isOpaque, err = checkOpaque(upperdir, path, base, f)
			if err != nil {
				return err
			}
			// Avoid including directory that hasn't been modified. If /foo/bar/baz is modified,
			// then /foo will apper here even if it's not been modified because it's the parent of bar.
			if same, err := sameDir(baseF, f, filepath.Join(base, path), filepath.Join(upperdirView, path)); same {
				skipRecord = true // Both are the same, don't record the change
			} else if err != nil {
				return err
			}
		} else if os.IsNotExist(err) {
			// File doesn't exist in the base layer. Thus this is added.
			kind = fs.ChangeKindAdd
		} else if err != nil {
			return err
		}

		if isOpaque {
			if err := changeFn(fs.ChangeKindDelete, path, nil, nil); err != nil {
				return err
			}
		}
		if !skipRecord {
			if err := changeFn(kind, path, f, nil); err != nil {
				return err
			}
		}

		if f != nil {
			if isOpaque {
				// This is an opaque directory. Start a new walking differ to get adds/deletes of
				// this directory. We use "upperdirView" directory which doesn't contain whiteouts.
				if err := fs.Changes(ctx, filepath.Join(base, path), filepath.Join(upperdirView, path),
					func(k fs.ChangeKind, p string, f os.FileInfo, err error) error {
						return changeFn(k, filepath.Join(path, p), f, err) // rebase path to be based on the opaque dir
					},
				); err != nil {
					return err
				}
				return filepath.SkipDir // We completed this directory. Do not walk files under this directory anymore.
			}
		}
		return nil
	})
}

// checkDelete checks if the specified file is a whiteout
func checkDelete(upperdir string, path string, base string, f os.FileInfo) (delete, skip bool, _ error) {
	if f.Mode()&os.ModeCharDevice != 0 {
		if _, ok := f.Sys().(*syscall.Stat_t); ok {
			maj, min, err := devices.DeviceInfo(f)
			if err != nil {
				return false, false, errors.Wrapf(err, "failed to get device info")
			}
			if maj == 0 && min == 0 {
				// This file is a whiteout (char 0/0) that indicates this is deleted from the base
				if _, err := os.Lstat(filepath.Join(base, path)); err != nil {
					if !os.IsNotExist(err) {
						return false, false, errors.Wrapf(err, "failed to lstat")
					}
					// This file doesn't exist even in the base dir.
					// We don't need whiteout. Just skip this file.
					return false, true, nil
				}
				return true, false, nil
			}
		}
	}
	return false, false, nil
}

// checkDelete checks if the specified file is an opaque directory
func checkOpaque(upperdir string, path string, base string, f os.FileInfo) (isOpaque bool, _ error) {
	if f.IsDir() {
		for _, oKey := range []string{"trusted.overlay.opaque", "user.overlay.opaque"} {
			opaque, err := sysx.LGetxattr(filepath.Join(upperdir, path), oKey)
			if err != nil && err != unix.ENODATA && err != unix.ENOTSUP {
				return false, errors.Wrapf(err, "failed to retrieve %s attr", oKey)
			} else if len(opaque) == 1 && opaque[0] == 'y' {
				// This is an opaque whiteout directory.
				if _, err := os.Lstat(filepath.Join(base, path)); err != nil {
					if !os.IsNotExist(err) {
						return false, errors.Wrapf(err, "failed to lstat")
					}
					// This file doesn't exist even in the base dir. We don't need treat this as an opaque.
					return false, nil
				}
				return true, nil
			}
		}
	}
	return false, nil
}

// sameDir performs continity-compatible comparison of directories.
// https://github.com/containerd/continuity/blob/v0.1.0/fs/path.go#L91-L133
// This doesn't compare files because it requires to compare their contents.
// This is what we want to avoid by this overlayfs-specialized differ.
func sameDir(f1, f2 os.FileInfo, f1fullPath, f2fullPath string) (bool, error) {
	if !f1.IsDir() || !f2.IsDir() {
		return false, nil
	}

	if os.SameFile(f1, f2) {
		return true, nil
	}

	equalStat, err := compareSysStat(f1.Sys(), f2.Sys())
	if err != nil || !equalStat {
		return equalStat, err
	}

	if eq, err := compareCapabilities(f1fullPath, f2fullPath); err != nil || !eq {
		return eq, err
	}

	return true, nil
}

// Ported from continuity project
// https://github.com/containerd/continuity/blob/v0.1.0/fs/diff_unix.go#L43-L54
// Copyright The containerd Authors.
func compareSysStat(s1, s2 interface{}) (bool, error) {
	ls1, ok := s1.(*syscall.Stat_t)
	if !ok {
		return false, nil
	}
	ls2, ok := s2.(*syscall.Stat_t)
	if !ok {
		return false, nil
	}

	return ls1.Mode == ls2.Mode && ls1.Uid == ls2.Uid && ls1.Gid == ls2.Gid && ls1.Rdev == ls2.Rdev, nil
}

// Ported from continuity project
// https://github.com/containerd/continuity/blob/v0.1.0/fs/diff_unix.go#L56-L66
// Copyright The containerd Authors.
func compareCapabilities(p1, p2 string) (bool, error) {
	c1, err := sysx.LGetxattr(p1, "security.capability")
	if err != nil && err != sysx.ENODATA {
		return false, errors.Wrapf(err, "failed to get xattr for %s", p1)
	}
	c2, err := sysx.LGetxattr(p2, "security.capability")
	if err != nil && err != sysx.ENODATA {
		return false, errors.Wrapf(err, "failed to get xattr for %s", p2)
	}
	return bytes.Equal(c1, c2), nil
}
