package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/sys/unix"

	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/docker/docker/pkg/idtools"

	"github.com/moby/buildkit/util/appdefaults"
)

type Mountable interface {
	// ID() string
	Mount() ([]mount.Mount, func() error, error)
	IdentityMapping() *idtools.IdentityMapping
}

// Snapshotter defines interface that any snapshot implementation should satisfy
type Snapshotter interface {
	Name() string
	Mounts(ctx context.Context, key string) (Mountable, error)
	Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) error
	View(ctx context.Context, key, parent string, opts ...snapshots.Opt) (Mountable, error)
	Merge(ctx context.Context, parents []string) (Mountable, func() error, error)

	Stat(ctx context.Context, key string) (snapshots.Info, error)
	Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error)
	Usage(ctx context.Context, key string) (snapshots.Usage, error)
	Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error
	Remove(ctx context.Context, key string) error
	Walk(ctx context.Context, fn snapshots.WalkFunc, filters ...string) error
	Close() error
	IdentityMapping() *idtools.IdentityMapping
}

func FromContainerdSnapshotter(name string, s snapshots.Snapshotter, idmap *idtools.IdentityMapping) Snapshotter {
	return &fromContainerd{name: name, Snapshotter: s, idmap: idmap}
}

type fromContainerd struct {
	name string
	snapshots.Snapshotter
	idmap *idtools.IdentityMapping
}

func (s *fromContainerd) Name() string {
	return s.name
}

type MergeNotSupportedError struct {
	Err error
}

func (e MergeNotSupportedError) Error() string {
	return fmt.Sprintf("merging snapshots is not supported: %s", e.Err)
}

func (e MergeNotSupportedError) Unwrap() error {
	return e.Err
}

type mergeMount struct {
	Lowerdirs []string
	Upperdir  string
	Workdir   string
	Userxattr bool
	Options   []string
}

func (m mergeMount) Merge(other *mergeMount) (*mergeMount, error) {
	if other == nil {
		return &m, nil
	}
	m.Lowerdirs = append(m.Lowerdirs, other.Lowerdirs...)
	if m.Upperdir != "" && other.Upperdir != "" {
		return nil, fmt.Errorf("TODO real err")
	}
	if m.Upperdir == "" {
		m.Upperdir = other.Upperdir
		m.Workdir = other.Workdir
	}
	m.Options = append(other.Options, m.Options...)
	return &m, nil
}

func parseMount(m mount.Mount) (*mergeMount, error) {
	if m.Type != "overlay" && m.Type != "bind" && m.Type != "" {
		return nil, MergeNotSupportedError{Err: fmt.Errorf("invalid mount type: %s", m.Type)}
	}

	mm := &mergeMount{}
	var isReadonly bool
	for _, opt := range m.Options {
		kv := strings.SplitN(opt, "=", 2)
		if len(kv) == 1 {
			kv = append(kv, "")
		}
		switch k, v := kv[0], kv[1]; k {
		case "lowerdir":
			mm.Lowerdirs = strings.Split(v, ":")
		case "upperdir":
			mm.Upperdir = v
		case "workdir":
			mm.Workdir = v
		case "index":
			if v != "off" {
				return nil, fmt.Errorf("TODO real error")
			}
		case "metacopy":
			if v != "off" {
				return nil, fmt.Errorf("TODO real error")
			}
		case "userxattr":
			mm.Userxattr = true
		case "bind", "rbind":
			mm.Upperdir = m.Source
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
			mm.Options = append(mm.Options, opt)
		}
	}

	// if we encountered a (r)bind option and then later an "ro" option, then we need to ensure we set the bind source
	// as a read-only lowerdir, not upperdir
	if isReadonly && mm.Upperdir != "" {
		mm.Lowerdirs = append([]string{mm.Upperdir}, mm.Lowerdirs...)
		mm.Upperdir = ""
		mm.Workdir = ""
	}

	return mm, nil
}

// TODO outstanding issues (may not all need to be addressed):
// * Might be worth including a "fast" cleanup arg to the cleanup func to indicate that you can just throw it all away
//   rather than try to fix anything up
// * Options are joined together, with higher snapshots options overriding lowers. This means that if a lower layer
//   sets "noexec" but a higher one sets "exec", then "exec" will be preferred. Snapshotters are thus expected to always
//   set the same set of values for options like (no)exec, (no)dev, no(suid) or they will get undefined results. As of
//   right now, I am not aware of any snapshotters that return any such options.
// * Options are not deduplicated between joined mounts, so it's possible for them to take up more space than necessary
//   in the "data" arg to the mount syscall, which could cause it to reach its 1 page size limit earlier.
func (s *fromContainerd) Merge(ctx context.Context, parents []string) (Mountable, func() error, error) {
	var cleanupFuncs []func() error
	doCleanup := func() error {
		var err error
		for i := range cleanupFuncs {
			f := cleanupFuncs[len(cleanupFuncs)-1-i]
			err = multierror.Append(err, f()).ErrorOrNil()
		}
		return err
	}

	if len(parents) == 0 {
		return &staticMountable{
			mounts: []mount.Mount{},
			idmap:  s.idmap,
			id:     "TODO", // TODO what to use here? we don't have an ID and this field seems to only be used for logging
		}, doCleanup, nil
	}

	mm := &mergeMount{}
	for _, key := range parents {
		mounts, err := s.Snapshotter.Mounts(ctx, key)
		if err != nil {
			return nil, nil, err
		}
		for _, m := range mounts {
			newMM, err := parseMount(m)
			if err != nil {
				return nil, nil, err
			}
			mm, err = mm.Merge(newMM)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	// single read-only layer, return a ro bind mount
	if mm.Upperdir == "" && len(mm.Lowerdirs) == 1 {
		return &staticMountable{
			mounts: []mount.Mount{{
				Type:   "bind",
				Source: mm.Lowerdirs[0],
				Options: append(mm.Options,
					"rbind",
					"ro",
				),
			}},
			idmap: s.idmap,
			id:    "TODO", // TODO what to use here? we don't have an ID and this field seems to only be used for logging
		}, doCleanup, nil
	}

	// single read-write layer, return a rw bind mount
	if mm.Upperdir != "" && len(mm.Lowerdirs) == 0 {
		return &staticMountable{
			mounts: []mount.Mount{{
				Type:   "bind",
				Source: mm.Upperdir,
				Options: append(mm.Options,
					"rbind",
					"rw",
				),
			}},
			idmap: s.idmap,
			id:    "TODO", // TODO what to use here? we don't have an ID and this field seems to only be used for logging
		}, doCleanup, nil
	}

	// multiple layers, return an overlay mount
	if mm.Upperdir != "" {
		cleanupFuncs = append(cleanupFuncs, func() error {
			var opaqueXattrName string
			if mm.Userxattr {
				opaqueXattrName = "user.overlay.opaque"
			} else {
				opaqueXattrName = "trusted.overlay.opaque"
			}

			var opaquePaths []string
			if err := filepath.Walk(mm.Upperdir, func(path string, info os.FileInfo, err error) error {
				if !info.IsDir() {
					return nil
				}
				dest := make([]byte, 1)
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
				return nil
			}); err != nil {
				return err
			}

			for _, opaquePath := range opaquePaths {
				relPath, err := filepath.Rel(mm.Upperdir, opaquePath)
				if err != nil {
					return err
				}
				var overridesLower bool
				for _, lowerdir := range mm.Lowerdirs {
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

		if mm.Workdir == "" {
			origUpperdir := mm.Upperdir
			mm.Workdir = filepath.Join(origUpperdir, "work")
			mm.Upperdir = filepath.Join(origUpperdir, "fs")
			cleanupFuncs = append(cleanupFuncs, func() error {
				if err := os.RemoveAll(mm.Workdir); err != nil {
					return err
				}
				if fileInfos, err := ioutil.ReadDir(mm.Upperdir); err != nil {
					return err
				} else {
					var handleSameNameDirent bool
					for _, fileInfo := range fileInfos {
						oldpath := filepath.Join(mm.Upperdir, fileInfo.Name())
						newpath := filepath.Join(origUpperdir, fileInfo.Name())
						if oldpath == newpath {
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
						name := filepath.Base(mm.Upperdir)
						oldpath := filepath.Join(mm.Upperdir, name)
						temppath := filepath.Join(origUpperdir, name+".temp")
						finalpath := filepath.Join(origUpperdir, name)
						if err := os.Rename(oldpath, temppath); err != nil {
							return err
						}
						if err := os.RemoveAll(mm.Upperdir); err != nil {
							return err
						}
						if err := os.Rename(temppath, finalpath); err != nil {
							return err
						}
					}
				}
				return nil
			})
		}
	}

	if _, err := os.Stat("/sys/module/overlay/parameters/index"); err == nil {
		mm.Options = append(mm.Options, "index=off")
	}
	if _, err := os.Stat("/sys/module/overlay/parameters/metacopy"); err == nil {
		mm.Options = append(mm.Options, "metacopy=off")
	}

	if mm.Userxattr {
		mm.Options = append(mm.Options, "userxattr")
	}

	mm.Options = append(mm.Options, fmt.Sprintf("lowerdir=%s", strings.Join(mm.Lowerdirs, ":")))
	mm.Options = append(mm.Options, fmt.Sprintf("upperdir=%s", mm.Upperdir))
	mm.Options = append(mm.Options, fmt.Sprintf("workdir=%s", mm.Workdir))

	return &staticMountable{
		mounts: []mount.Mount{{
			Type:    "overlay",
			Source:  "overlay",
			Options: mm.Options,
		}},
		idmap: s.idmap,
		id:    "TODO", // TODO what to use here? we don't have an ID and this field seems to only be used for logging
	}, doCleanup, nil
}

func (s *fromContainerd) Mounts(ctx context.Context, key string) (Mountable, error) {
	mounts, err := s.Snapshotter.Mounts(ctx, key)
	if err != nil {
		return nil, err
	}
	return &staticMountable{mounts: mounts, idmap: s.idmap, id: key}, nil
}
func (s *fromContainerd) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) error {
	_, err := s.Snapshotter.Prepare(ctx, key, parent, opts...)
	return err
}
func (s *fromContainerd) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) (Mountable, error) {
	mounts, err := s.Snapshotter.View(ctx, key, parent, opts...)
	if err != nil {
		return nil, err
	}
	return &staticMountable{mounts: mounts, idmap: s.idmap, id: key}, nil
}
func (s *fromContainerd) IdentityMapping() *idtools.IdentityMapping {
	return s.idmap
}

type staticMountable struct {
	count  int32
	id     string
	mounts []mount.Mount
	idmap  *idtools.IdentityMapping
}

func (cm *staticMountable) Mount() ([]mount.Mount, func() error, error) {
	atomic.AddInt32(&cm.count, 1)
	return cm.mounts, func() error {
		if atomic.AddInt32(&cm.count, -1) < 0 {
			if v := os.Getenv("BUILDKIT_DEBUG_PANIC_ON_ERROR"); v == "1" {
				panic("release of released mount " + cm.id)
			}
		}
		return nil
	}, nil
}

func (cm *staticMountable) IdentityMapping() *idtools.IdentityMapping {
	return cm.idmap
}

// NewContainerdSnapshotter converts snapshotter to containerd snapshotter
func NewContainerdSnapshotter(s Snapshotter) (snapshots.Snapshotter, func() error) {
	cs := &containerdSnapshotter{Snapshotter: s}
	return cs, cs.release
}

type containerdSnapshotter struct {
	mu        sync.Mutex
	releasers []func() error
	Snapshotter
}

func (cs *containerdSnapshotter) release() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	var err error
	for _, f := range cs.releasers {
		if err1 := f(); err1 != nil && err == nil {
			err = err1
		}
	}
	return err
}

func (cs *containerdSnapshotter) returnMounts(mf Mountable) ([]mount.Mount, error) {
	mounts, release, err := mf.Mount()
	if err != nil {
		return nil, err
	}
	cs.mu.Lock()
	cs.releasers = append(cs.releasers, release)
	cs.mu.Unlock()
	return mounts, nil
}

func (cs *containerdSnapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	mf, err := cs.Snapshotter.Mounts(ctx, key)
	if err != nil {
		return nil, err
	}
	return cs.returnMounts(mf)
}

func (cs *containerdSnapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	if err := cs.Snapshotter.Prepare(ctx, key, parent, opts...); err != nil {
		return nil, err
	}
	return cs.Mounts(ctx, key)
}
func (cs *containerdSnapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	mf, err := cs.Snapshotter.View(ctx, key, parent, opts...)
	if err != nil {
		return nil, err
	}
	return cs.returnMounts(mf)
}
