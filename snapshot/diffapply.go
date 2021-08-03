package snapshot

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/containerd/containerd/mount"
	"github.com/containerd/continuity/fs"
	"github.com/containerd/continuity/sysx"
	"github.com/moby/buildkit/util/bklog"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

func diffApplyMerge(ctx context.Context, lowerMounts, upperMounts, applyMounts []mount.Mount, useHardlink bool) error {
	if applyMounts == nil {
		return errors.New("invalid nil apply mounts")
	}

	withTempMount := func(ctx context.Context, mounts []mount.Mount, f func(root string) error) error {
		if mounts == nil {
			return f("")
		}
		if len(mounts) == 1 {
			mnt := mounts[0]
			if mnt.Type == "bind" || mnt.Type == "rbind" {
				return f(mnt.Source)
			}
		}
		return mount.WithTempMount(ctx, mounts, f)
	}

	if err := withTempMount(ctx, lowerMounts, func(lowerRoot string) error {
		return withTempMount(ctx, upperMounts, func(upperRoot string) error {
			return withTempMount(ctx, applyMounts, func(applyRoot string) error {
				type pathTime struct {
					path  string
					atime unix.Timespec
					mtime unix.Timespec
				}
				var times []pathTime
				err := fs.Changes(ctx, lowerRoot, upperRoot, func(kind fs.ChangeKind, changePath string, upperFi os.FileInfo, prevErr error) error {
					if prevErr != nil {
						return prevErr
					}

					applyPath := filepath.Join(applyRoot, changePath)
					upperPath := filepath.Join(upperRoot, changePath)

					if kind == fs.ChangeKindUnmodified {
						return nil
					}
					if kind == fs.ChangeKindDelete {
						if err := os.RemoveAll(applyPath); err != nil {
							return errors.Wrapf(err, "failed to remove path %s during apply", applyPath)
						}
						return nil
					}

					applyFi, err := os.Lstat(applyPath)
					if err != nil && !os.IsNotExist(err) {
						return errors.Wrapf(err, "failed to stat path %s during apply", applyPath)
					}

					// if there is an existing file/dir at the applyPath, delete it unless both it and upper are dirs (in which case they get merged)
					if applyFi != nil && !(applyFi.IsDir() && upperFi.IsDir()) {
						if err := os.RemoveAll(applyPath); err != nil {
							return errors.Wrapf(err, "failed to remove path %s during apply", applyPath)
						}
						applyFi = nil
					}

					// hardlink fast-path
					if !upperFi.IsDir() && useHardlink {
						if err := os.Link(upperPath, applyPath); err != nil {
							return errors.Wrapf(err, "failed to hardlink %q to %q during apply", upperPath, applyPath)
						}
						return nil
					}

					upperStat, ok := upperFi.Sys().(*syscall.Stat_t)
					if !ok {
						return fmt.Errorf("unhandled stat type for %+v", upperFi)
					}

					inodes := make(map[uint64]string)
					switch upperFi.Mode().Type() {
					case 0: // regular file
						if upperStat.Nlink > 1 {
							if linkedPath, ok := inodes[upperStat.Ino]; ok {
								if err := os.Link(linkedPath, applyPath); err != nil {
									return errors.Wrap(err, "failed to create hardlink during apply")
								}
								return nil // no other metadata updates needed when hardlinking
							}
							inodes[upperStat.Ino] = applyPath
						}
						if err := fs.CopyFile(applyPath, upperPath); err != nil {
							return errors.Wrapf(err, "failed to copy from %s to %s during apply", upperPath, applyPath)
						}
					case os.ModeDir:
						if applyFi == nil {
							// applyPath doesn't exist, make it a dir
							if err := os.Mkdir(applyPath, upperFi.Mode()); err != nil {
								return errors.Wrap(err, "failed to create applied dir")
							}
						}
					case os.ModeSymlink:
						if target, err := os.Readlink(upperPath); err != nil {
							return errors.Wrap(err, "failed to read symlink during apply")
						} else if err := os.Symlink(target, applyPath); err != nil {
							return errors.Wrap(err, "failed to create symlink during apply")
						}
					case os.ModeCharDevice:
						fallthrough
					case os.ModeDevice:
						if err := unix.Mknod(applyPath, uint32(upperFi.Mode()), int(upperStat.Rdev)); err != nil {
							return errors.Wrap(err, "failed to create device during apply")
						}
					case os.ModeNamedPipe:
						fallthrough
					case os.ModeSocket:
						// TODO: this behavior is different from the efficient case and the hardlink fast-path case
						bklog.G(ctx).Debugf("ignoring file %s (%s) during apply", upperFi.Name(), upperFi.Mode().String())
						return nil
					case os.ModeIrregular:
						bklog.G(ctx).Errorf("unhandled irregular file %s (%s) during apply", upperFi.Name(), upperFi.Mode().String())
						return nil
					}

					if err := os.Lchown(applyPath, int(upperStat.Uid), int(upperStat.Gid)); err != nil {
						return errors.Wrap(err, "failed to chown applied dir")
					}

					xattrVal, err := sysx.LGetxattr(upperPath, securityCapabilityXattr)
					if err != nil && err != unix.ENODATA {
						return errors.Wrap(err, "failed to get security capability xattrs")
					} else if err != unix.ENODATA {
						if err := sysx.LSetxattr(applyPath, securityCapabilityXattr, xattrVal, 0); err != nil {
							// This can often fail, so just log it: https://github.com/moby/buildkit/issues/1189
							// TODO: should this try all xattrs?
							bklog.G(ctx).Debugf("failed to set security capability xattr of path %s during apply", applyPath)
						}
					}

					// run chmod unconditionally to ensure masks enforced in previous calls didn't interfere
					if err := os.Chmod(applyPath, upperFi.Mode()); err != nil {
						return errors.Wrap(err, "failed to chmod applied dir")
					}

					// save the times we should set on this path, to be applied at the end
					times = append(times, pathTime{
						path: applyPath,
						atime: unix.Timespec{
							Sec:  upperStat.Atim.Sec,
							Nsec: upperStat.Atim.Nsec,
						},
						mtime: unix.Timespec{
							Sec:  upperStat.Mtim.Sec,
							Nsec: upperStat.Mtim.Nsec,
						},
					})

					return nil
				})
				if err != nil {
					return err
				}

				// Set times now that everything has been modified. Iterate in reverse so that mtime changes to parent dirs
				// created while updating children will get overridden with the correct values
				for i := range times {
					ts := times[len(times)-1-i]
					if err := unix.UtimesNanoAt(unix.AT_FDCWD, ts.path, []unix.Timespec{ts.atime, ts.mtime}, unix.AT_SYMLINK_NOFOLLOW); err != nil {
						return errors.Wrapf(err, "failed to update times of path %q", ts.path)
					}
				}

				return nil
			})
		})
	}); err != nil {
		return errors.Wrapf(err, "failed to apply diff")
	}
	return nil
}
