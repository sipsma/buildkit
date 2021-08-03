package snapshot

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/overlay/overlayutils"
	"github.com/docker/docker/pkg/idtools"
	"github.com/hashicorp/go-multierror"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/util/bklog"
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
func needsUserXAttr(ctx context.Context, sn snapshots.Snapshotter) (bool, error) {
	key := identity.NewID()
	mnts, err := sn.Prepare(ctx, key, "")
	if err != nil {
		return false, err
	}
	defer func() {
		if err := sn.Remove(ctx, key); err != nil {
			bklog.G(ctx).Errorf("failed to remove temp snapshot while checking userxattr: %v", err)
		}
	}()

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

type mergedOverlay struct {
	parents   []mount.Mount
	idmap     *idtools.IdentityMapping
	userxattr bool
}

func (m mergedOverlay) Mount() (rmnts []mount.Mount, _ func() error, rerr error) {
	type overlay struct {
		lowerdirs []string
		upperdir  string
		workdir   string
		options   []string
	}

	merge := func(a, b *overlay, retainUpper bool) (*overlay, error) {
		if a == nil {
			return nil, errors.New("invalid nil mount")
		}
		if b == nil {
			return nil, errors.New("invalid nil mount")
		}

		c := &overlay{}
		if !retainUpper && b.upperdir != "" {
			c.lowerdirs = []string{b.upperdir}
		}
		c.lowerdirs = append(c.lowerdirs, b.lowerdirs...)
		c.lowerdirs = append(c.lowerdirs, a.lowerdirs...)

		c.upperdir = a.upperdir
		c.workdir = a.workdir
		if retainUpper && b.upperdir != "" {
			if c.upperdir != "" {
				return nil, fmt.Errorf("cannot merge overlays with multiple upper dirs")
			}
			c.upperdir = b.upperdir
			c.workdir = b.workdir
		}

		// TODO(sipsma) optimize by de-duplicating options between mounts (most snapshotters don't return extra mount
		// options here, but if they did and the were duped, they would cause the 1-page size limit to be hit faster)
		c.options = append(c.options, b.options...)

		// De-dupe lowerdirs; if a lower appears more than once, then remove all occurrences besides the first one. This
		// is necessary for recent kernel overlay implementations that will return ELOOP if a lowerdir is repeated.
		var filteredLowers []string
		memo := map[string]struct{}{}
		if c.upperdir != "" {
			// also dedupe any occurences of the upperdir from lowerdirs
			memo[c.upperdir] = struct{}{}
		}
		for _, lower := range c.lowerdirs {
			if _, ok := memo[lower]; ok {
				continue
			}
			memo[lower] = struct{}{}
			filteredLowers = append(filteredLowers, lower)
		}
		c.lowerdirs = filteredLowers

		return c, nil
	}

	parse := func(mnt mount.Mount) (*overlay, error) {
		if mnt.Type != "overlay" && mnt.Type != "bind" && mnt.Type != "" {
			return nil, MergeNotSupportedError{Err: fmt.Errorf("invalid mount type: %s", mnt.Type)}
		}

		mm := &overlay{}
		var isReadonly bool
		for _, opt := range mnt.Options {
			kv := strings.SplitN(opt, "=", 2)
			if len(kv) == 1 {
				kv = append(kv, "")
			}
			switch k, v := kv[0], kv[1]; k {
			case "lowerdir":
				mm.lowerdirs = strings.Split(v, ":")
			case "upperdir":
				mm.upperdir = v
			case "workdir":
				mm.workdir = v
			case "index":
				if v != "off" {
					return nil, fmt.Errorf("overlay index option must be set to off")
				}
			case "metacopy":
				if v != "off" {
					return nil, fmt.Errorf("overlay metacopy option must be set to off")
				}
			case "userxattr":
				if !m.userxattr {
					return nil, fmt.Errorf("conflicting userxattr options")
				}
			case "bind", "rbind":
				mm.upperdir = mnt.Source
				// Note: In the obscure case where a snapshotter returns "rbind" and actually expects submounts to be
				// included with the bind, they will not be included if we return an overlay mount. This is due to the
				// fact that the kernel overlay implementation doesn't include submounts of lower/upper dirs when
				// constructing overlay mounts. As of this writing, there doesn't appear to be any such snapshotters
				// among the ones I'm aware of.
			case "ro":
				isReadonly = true
			case "rw":
				isReadonly = false
			case "remount":
				return nil, fmt.Errorf("cannot create mount with remount option set")
			default:
				mm.options = append(mm.options, opt)
			}
		}

		// if we encountered a (r)bind option and then later an "ro" option, then we need to ensure we set the bind source
		// as a read-only lowerdir, not upperdir
		if isReadonly && mm.upperdir != "" {
			mm.lowerdirs = append([]string{mm.upperdir}, mm.lowerdirs...)
			mm.upperdir = ""
			mm.workdir = ""
		}

		return mm, nil
	}

	if len(m.parents) == 0 {
		return nil, nil, fmt.Errorf("invalid merge mount with no parents")
	}

	// TODO(sipsma) optimize by adding "fast cleanup" arg to callback, set to true when all data can be thrown away
	var cleanupFuncs []func() error
	doCleanup := func() error {
		var err error
		for i := range cleanupFuncs {
			f := cleanupFuncs[len(cleanupFuncs)-1-i]
			err = multierror.Append(err, f()).ErrorOrNil()
		}
		return err
	}
	defer func() {
		if rerr != nil {
			if err := doCleanup(); err != nil {
				bklog.G(context.TODO()).Errorf("failed to cleanup mount: %v", err)
			}
		}
	}()

	mm := &overlay{}
	for i, parent := range m.parents {
		newMM, err := parse(parent)
		if err != nil {
			return nil, nil, err
		}
		mm, err = merge(mm, newMM, i == len(m.parents)-1) // retain the upperdir only of the last parent
		if err != nil {
			return nil, nil, err
		}
	}

	// single read-only layer, return a ro bind mount
	if mm.upperdir == "" && len(mm.lowerdirs) == 1 {
		return []mount.Mount{{
			Type:   "bind",
			Source: mm.lowerdirs[0],
			Options: append(mm.options,
				"rbind",
				"ro",
			),
		}}, doCleanup, nil
	}

	// single read-write layer, return a rw bind mount
	if mm.upperdir != "" && len(mm.lowerdirs) == 0 {
		return []mount.Mount{{
			Type:   "bind",
			Source: mm.upperdir,
			Options: append(mm.options,
				"rbind",
				"rw",
			),
		}}, doCleanup, nil
	}

	// multiple layers, return an overlay mount

	if mm.upperdir != "" {
		if mm.workdir == "" {
			// Corner case where we are returning an overlay mount w/ an upperdir, but don't have a workdir because
			// our upperdir was originally just a bindmount. Deal with it by creating temporary upper+work dirs within
			// the real upper (to ensure that the upper and work dirs are on the same filesystem) and then fix the
			// upperdir up once unmounted with the cleanup callback.
			origUpperdir := mm.upperdir
			tempupper, err := os.MkdirTemp(origUpperdir, "")
			if err != nil {
				return nil, nil, errors.Wrap(err, "failed to create temporary upper dir for overlay")
			}
			mm.upperdir = tempupper
			upperFileInfos, err := os.ReadDir(origUpperdir)
			if err != nil {
				return nil, nil, errors.Wrap(err, "failed to reader upperdir")
			}
			for _, fi := range upperFileInfos {
				if filepath.Base(fi.Name()) == filepath.Base(tempupper) {
					continue
				}
				if err := os.Rename(filepath.Join(origUpperdir, fi.Name()), filepath.Join(mm.upperdir, fi.Name())); err != nil {
					return nil, nil, errors.Wrap(err, "failed to rename upperdir ent to temporary path")
				}
			}
			mm.workdir = filepath.Join(origUpperdir, "work")
			if err := os.Mkdir(mm.workdir, 0700); err != nil {
				return nil, nil, errors.Wrap(err, "failed to create temporary work dir for overlay")
			}

			cleanupFuncs = append(cleanupFuncs, func() error {
				if err := os.RemoveAll(mm.workdir); err != nil {
					return err
				}
				fileInfos, err := os.ReadDir(mm.upperdir)
				if err != nil {
					return err
				}
				var handleSameNameDirent bool
				for _, fileInfo := range fileInfos {
					oldpath := filepath.Join(mm.upperdir, fileInfo.Name())
					newpath := filepath.Join(origUpperdir, fileInfo.Name())
					if newpath == mm.upperdir {
						// Corner case where there was a file/dir created on upperdir with the same name as the
						// upperdir, handle by putting off moving this dirent until the very end so we can, at that
						// time, move it out with a special temp name, remove the upperdir and then rename it to its
						// final correct name
						handleSameNameDirent = true
						continue
					}
					if err := os.Rename(oldpath, newpath); err != nil {
						return err
					}
				}
				if handleSameNameDirent {
					name := filepath.Base(mm.upperdir)
					oldpath := filepath.Join(mm.upperdir, name)
					finalpath := filepath.Join(origUpperdir, name)
					// use os.CreateTemp to find an unused random name
					tempfile, err := os.CreateTemp(origUpperdir, "")
					if err != nil {
						return err
					}
					if err := os.RemoveAll(tempfile.Name()); err != nil {
						return err
					}
					if err := os.Rename(oldpath, tempfile.Name()); err != nil {
						return err
					}
					if err := os.RemoveAll(mm.upperdir); err != nil {
						return err
					}
					if err := os.Rename(tempfile.Name(), finalpath); err != nil {
						return err
					}
				} else {
					if err := os.RemoveAll(mm.upperdir); err != nil {
						return err
					}
				}
				return nil
			})
		}

		cleanupFuncs = append(cleanupFuncs, func() error {
			// Corner case where dirs get set as opaque when they are newly created with no corresponding lower dir.
			// This is a performance optimization by the kernel but does not matter for functionality and causes
			// problems for us, so remove any such opaque xattrs (while leaving opaque xattrs that are actually important).
			var opaqueXattrName string
			if m.userxattr {
				opaqueXattrName = userOpaqueXattr
			} else {
				opaqueXattrName = trustedOpaqueXattr
			}

			var opaquePaths []string
			if err := filepath.WalkDir(mm.upperdir, func(path string, info fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !info.IsDir() {
					return nil
				}
				dest := make([]byte, 1)
				if _, err := unix.Lgetxattr(path, opaqueXattrName, dest); err != nil {
					if err == unix.ENODATA {
						// isn't marked as opaque
						return nil
					}
					return err
				}
				// it has the opaque xattr
				opaquePaths = append(opaquePaths, path)
				return fs.SkipDir // can skip subdirs, they can't also be opaque
			}); err != nil {
				return err
			}

			for _, opaquePath := range opaquePaths {
				relPath, err := filepath.Rel(mm.upperdir, opaquePath)
				if err != nil {
					return err
				}
				var overridesLower bool
				for _, lowerdir := range mm.lowerdirs {
					if _, err := os.Lstat(filepath.Join(lowerdir, relPath)); os.IsNotExist(err) {
						continue
					} else if err != nil {
						return err
					}
					overridesLower = true
					break
				}
				if !overridesLower {
					if err := unix.Lremovexattr(opaquePath, opaqueXattrName); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}

	if _, err := os.Stat("/sys/module/overlay/parameters/index"); err == nil {
		mm.options = append(mm.options, "index=off")
	}
	if _, err := os.Stat("/sys/module/overlay/parameters/metacopy"); err == nil {
		mm.options = append(mm.options, "metacopy=off")
	}

	if m.userxattr {
		mm.options = append(mm.options, "userxattr")
	}

	mm.options = append(mm.options, fmt.Sprintf("lowerdir=%s", strings.Join(mm.lowerdirs, ":")))
	if mm.upperdir != "" {
		mm.options = append(mm.options, fmt.Sprintf("upperdir=%s", mm.upperdir))
		mm.options = append(mm.options, fmt.Sprintf("workdir=%s", mm.workdir))
	}

	return []mount.Mount{{
		Type:    "overlay",
		Source:  "overlay",
		Options: mm.options,
	}}, doCleanup, nil
}

func (m mergedOverlay) IdentityMapping() *idtools.IdentityMapping {
	return m.idmap
}

type MergeNotSupportedError struct {
	Err error
}

func (e MergeNotSupportedError) Error() string {
	return fmt.Sprintf("merging snapshots is not supported: %v", e.Err)
}

func (e MergeNotSupportedError) Unwrap() error {
	return e.Err
}
