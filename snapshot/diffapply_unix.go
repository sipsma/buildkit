// +build !windows

package snapshot

import (
	"context"
	"fmt"
	gofs "io/fs"
	"os"
	"path/filepath"
	"syscall"

	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/continuity/fs"
	"github.com/containerd/continuity/sysx"
	"github.com/moby/buildkit/util/bklog"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

const securityCapabilityXattr = "security.capability"

// diffApply calculates the diff between two directories and directly applies the changes to a separate mount (without using
// the content store as an intermediary). If useHardlink is set to true, it will hardlink non-directories instead of copying
// them when applying. This obviously requires that each of the mounts provided are for immutable, committed snapshots.
// externalHardlinks tracks any such hardlinks, which is needed for doing correct disk usage calculations elsewhere.
func diffApply(ctx context.Context, lowerMounts, upperMounts, applyMounts []mount.Mount, useHardlink bool, externalHardlinks map[uint64]struct{}) error {
	if applyMounts == nil {
		return errors.New("invalid nil apply mounts")
	}

	if err := withTempMount(ctx, lowerMounts, func(lowerRoot string) error {
		return withTempMount(ctx, upperMounts, func(upperRoot string) error {
			return withTempMount(ctx, applyMounts, func(applyRoot string) error {
				type pathTime struct {
					applyPath string
					atime     unix.Timespec
					mtime     unix.Timespec
				}

				// times holds the paths+times we visited and need to set
				var times []pathTime

				visited := make(map[string]struct{})
				inodes := make(map[uint64]string)
				err := fs.Changes(ctx, lowerRoot, upperRoot, func(kind fs.ChangeKind, changePath string, upperFi os.FileInfo, prevErr error) error {
					if prevErr != nil {
						return prevErr
					}

					applyPath := filepath.Join(applyRoot, changePath)
					upperPath := filepath.Join(upperRoot, changePath)
					visited[upperPath] = struct{}{}

					if kind == fs.ChangeKindUnmodified {
						return nil
					}
					if kind == fs.ChangeKindDelete {
						if err := os.RemoveAll(applyPath); err != nil {
							return errors.Wrapf(err, "failed to remove path %s during apply", applyPath)
						}
						return nil
					}

					upperStat, ok := upperFi.Sys().(*syscall.Stat_t)
					if !ok {
						return fmt.Errorf("unhandled stat type for %+v", upperFi)
					}

					// Check to see if we should reset a parent dir's times. This is needed when we are visiting a dirent
					// that changes but never visit its parent dir because the parent did not change
					upperParent := filepath.Dir(upperPath)
					if upperParent != upperRoot {
						if _, ok := visited[upperParent]; !ok {
							visited[upperParent] = struct{}{}
							parentInfo, err := os.Lstat(upperParent)
							if err != nil {
								return err
							}
							parentStat := parentInfo.Sys().(*syscall.Stat_t)
							times = append(times, pathTime{
								applyPath: filepath.Dir(applyPath),
								atime: unix.Timespec{
									Sec:  parentStat.Atim.Sec,
									Nsec: parentStat.Atim.Nsec,
								},
								mtime: unix.Timespec{
									Sec:  parentStat.Mtim.Sec,
									Nsec: parentStat.Mtim.Nsec,
								},
							})
						}
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
						// TODO:(sipsma) consider handling EMLINK by falling back to copy
						if err := os.Link(upperPath, applyPath); err != nil {
							return errors.Wrapf(err, "failed to hardlink %q to %q during apply", upperPath, applyPath)
						}
						// mark this inode as one coming from a separate snapshot, needed for disk usage calculations elsewhere
						externalHardlinks[upperStat.Ino] = struct{}{}
						return nil
					}

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
							bklog.G(ctx).Debugf("failed to set security capability xattr of path %s during apply", applyPath)
						}
					}

					if upperFi.Mode().Type() != os.ModeSymlink {
						if err := os.Chmod(applyPath, upperFi.Mode()); err != nil {
							return errors.Wrap(err, "failed to chmod applied dir")
						}
					}

					// save the times we should set on this path, to be applied at the end.
					times = append(times, pathTime{
						applyPath: applyPath,
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

				// Set times now that everything has been modified.
				for i := range times {
					ts := times[len(times)-1-i]
					if err := unix.UtimesNanoAt(unix.AT_FDCWD, ts.applyPath, []unix.Timespec{ts.atime, ts.mtime}, unix.AT_SYMLINK_NOFOLLOW); err != nil {
						return errors.Wrapf(err, "failed to update times of path %q", ts.applyPath)
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

// diskUsage calculates the disk space used by the provided mounts, similar to the normal containerd snapshotter disk usage
// calculations but with the extra ability to take into account hardlinks that were created between snapshots, ensuring that
// they don't get double counted.
func diskUsage(ctx context.Context, mounts []mount.Mount, externalHardlinks map[uint64]struct{}) (snapshots.Usage, error) {
	inodes := make(map[uint64]struct{})
	var usage snapshots.Usage
	err := withTempMount(ctx, mounts, func(root string) error {
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
	})
	if err != nil {
		return snapshots.Usage{}, err
	}
	return usage, nil
}
