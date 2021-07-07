package snapshot

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd/mount"
	"github.com/docker/docker/pkg/idtools"
	"github.com/hashicorp/go-multierror"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

type mergedMount struct {
	parents []mount.Mount
	idmap   *idtools.IdentityMapping
}

func (m mergedMount) Mount() (_ []mount.Mount, _ func() error, rerr error) {
	// TODO outstanding issues (may not all need to be addressed):
	// * De-dupe this code with the other stuff ktock has been working on
	// * Might be worth including a "fast" cleanup arg to the cleanup func to indicate that you can just throw it all away
	//   rather than try to fix anything up
	// * Options are joined together, with higher snapshots options overriding lowers. This means that if a lower layer
	//   sets "noexec" but a higher one sets "exec", then "exec" will be preferred. Snapshotters are thus expected to always
	//   set the same set of values for options like (no)exec, (no)dev, no(suid) or they will get undefined results. As of
	//   right now, I am not aware of any snapshotters that return any such options.
	// * Options are not deduplicated between joined mounts, so it's possible for them to take up more space than necessary
	//   in the "data" arg to the mount syscall, which could cause it to reach its 1 page size limit earlier.

	type merged struct {
		lowerdirs []string
		upperdir  string
		workdir   string
		userxattr bool
		options   []string
	}

	merge := func(a, b *merged, retainUpper bool) (*merged, error) {
		if a == nil {
			return b, nil
		}
		if b == nil {
			return a, nil
		}
		c := &merged{
			lowerdirs: append([]string{}, b.lowerdirs...), // start with b because overlay inverses order of lowerdirs...
			upperdir:  a.upperdir,
			workdir:   a.workdir,
			userxattr: a.userxattr,
			options:   append([]string{}, a.options...),
		}
		c.lowerdirs = append(c.lowerdirs, a.lowerdirs...)
		if b.upperdir != "" {
			if retainUpper {
				if c.upperdir != "" {
					return nil, fmt.Errorf("cannot merge overlays with multiple upper dirs")
				}
				c.upperdir = b.upperdir
				c.workdir = b.workdir
			} else {
				c.lowerdirs = append(c.lowerdirs, b.upperdir)
			}
		}
		if b.userxattr != c.userxattr {
			return nil, fmt.Errorf("cannot merged overlays with different userxattr options")
		}
		c.options = append(c.options, b.options...)
		return c, nil
	}

	parse := func(m mount.Mount) (*merged, error) {
		if m.Type != "overlay" && m.Type != "bind" && m.Type != "" {
			return nil, MergeNotSupportedError{Err: fmt.Errorf("invalid mount type: %s", m.Type)}
		}

		mm := &merged{}
		var isReadonly bool
		for _, opt := range m.Options {
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
					return nil, fmt.Errorf("TODO real error")
				}
			case "metacopy":
				if v != "off" {
					return nil, fmt.Errorf("TODO real error")
				}
			case "userxattr":
				mm.userxattr = true
			case "bind", "rbind":
				mm.upperdir = m.Source
				// TODO add a debug log when rbind option is ignored, which in theory could only be useful
				// in the obscure case where an arbitrary remote snapshotter relies on submounts being included
				// along with a bind mount it returns. In such a case, if we end up returning an overlay mount, any
				// lower or upper dirs that come from an rbind mount that actually posessess submounts won't have its
				// submounts included due to the implementation of overlay in the kernel. However, I am not aware of
				// any such snapshotter actually existing in practice.
			case "ro":
				isReadonly = true
			case "rw":
				isReadonly = false
			case "remount":
				return nil, fmt.Errorf("TODO real error")
			case "nfs_export":
				// TODO anything?
			case "redirect_dir":
				// TODO anything?
			case "xino":
				// TODO anything?
			case "uuid":
				// TODO anything?
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
			// TODO log
			// TODO do we actually want to do every cleanup?
			doCleanup()
		}
	}()

	if len(m.parents) == 0 {
		// TODO
		panic("TODO handle no parents")
	}

	mm := &merged{}
	for i, parent := range m.parents {
		newMM, err := parse(parent)
		if err != nil {
			return nil, nil, err
		}
		mm, err = merge(mm, newMM, i == len(m.parents)-1)
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
		cleanupFuncs = append(cleanupFuncs, func() error {
			var opaqueXattrName string
			if mm.userxattr {
				opaqueXattrName = "user.overlay.opaque"
			} else {
				opaqueXattrName = "trusted.overlay.opaque"
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
				// TODO check actual value of it?
				if _, err := unix.Lgetxattr(path, opaqueXattrName, dest); err != nil {
					if err == unix.ENODATA {
						// isn't marked as opaque
						return nil
					}
					// TODO handle E2BIG/ENOTSUP/ERANGE?
					return err
				}
				// it has the opaque xattr
				opaquePaths = append(opaquePaths, path)
				return fs.SkipDir // can skip subdirs, they can't also be opaque
			}); err != nil {
				return err
			}

			// TODO double check this... consider tricky cases like where the directories below have a complicated history of creation+deletion
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

		if mm.workdir == "" {
			origUpperdir := mm.upperdir
			mm.upperdir = filepath.Join(origUpperdir, "fs")
			if err := os.Mkdir(mm.upperdir, 0700); err != nil {
				return nil, nil, err
			}
			mm.workdir = filepath.Join(origUpperdir, "work")
			if err := os.Mkdir(mm.workdir, 0700); err != nil {
				return nil, nil, err
			}
			cleanupFuncs = append(cleanupFuncs, func() error {
				if err := os.RemoveAll(mm.workdir); err != nil {
					return err
				}
				fileInfos, err := ioutil.ReadDir(mm.upperdir)
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
					tempfile, err := os.CreateTemp(origUpperdir, "")
					if err != nil {
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
					if err := os.RemoveAll(tempfile.Name()); err != nil {
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
	}

	if _, err := os.Stat("/sys/module/overlay/parameters/index"); err == nil {
		mm.options = append(mm.options, "index=off")
	}
	if _, err := os.Stat("/sys/module/overlay/parameters/metacopy"); err == nil {
		mm.options = append(mm.options, "metacopy=off")
	}

	if mm.userxattr {
		mm.options = append(mm.options, "userxattr")
	}

	mm.options = append(mm.options, fmt.Sprintf("lowerdir=%s", strings.Join(mm.lowerdirs, ":")))
	if mm.upperdir != "" {
		mm.options = append(mm.options, fmt.Sprintf("upperdir=%s", mm.upperdir))
		mm.options = append(mm.options, fmt.Sprintf("workdir=%s", mm.workdir))
	}

	// TODO
	// TODO
	// TODO
	// TODO
	// TODO
	logrus.Debugf("merged mounts: %+v", mm.options)

	return []mount.Mount{{
		Type:    "overlay",
		Source:  "overlay",
		Options: mm.options,
	}}, doCleanup, nil
}

func (m mergedMount) IdentityMapping() *idtools.IdentityMapping {
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
