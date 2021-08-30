package snapshot

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/overlay/overlayutils"
	"github.com/docker/docker/pkg/idtools"
	"github.com/hashicorp/go-multierror"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/util/bklog"
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

// fixOpaqueDirs finds directories that have been marked opaque despite no dirents being located under them
// in lower dirs. This happens due to a opportunistic kernel optimization, but we don't it because it causes
// merges to have unnecessary opaque conflicts.
func fixOpaqueDirs(ctx context.Context, mounts []mount.Mount, userxattr bool) error {
	if len(mounts) != 1 {
		return errors.New("fixing opaque dirs only supports single mount lists")
	}
	m := mounts[0]
	// make a copy of options so modifications to it don't affect passed parameter
	m.Options = append([]string{}, m.Options...)

	var fixupDir string    // the dir we will check for unneeded opaques
	var lowerdirs []string // the extracted lowerdirs that will be checked to see if a given opaque is needed or not
	for i, opt := range m.Options {
		if v := strings.SplitN(opt, "lowerdir=", 2); len(v) == 2 && v[0] == "" {
			lowerdirs = strings.Split(v[1], ":")
			if len(lowerdirs) < 2 {
				return errors.New("invalid mount for fixing opaque dirs")
			}
			fixupDir = lowerdirs[0]
			var emptyLowerdir string
			var filteredLowerdirs []string
			for _, lower := range lowerdirs[1:] {
				if dirents, err := os.ReadDir(lower); err != nil {
					return errors.Wrap(err, "failed to read dirents of lowerdir while fixing opaque dirs")
				} else if len(dirents) == 0 {
					emptyLowerdir = lower
					continue
				}
				filteredLowerdirs = append(filteredLowerdirs, lower)
			}
			if len(filteredLowerdirs) == 0 {
				filteredLowerdirs = []string{emptyLowerdir}
			}
			lowerdirs = filteredLowerdirs
			m.Options[i] = "lowerdir=" + strings.Join(lowerdirs, ":")
			continue
		}
		if strings.HasPrefix(opt, "upperdir=") {
			return errors.New("fixing opaque dirs only supports read-only overlays")
		}
		if opt == "bind" || opt == "rbind" {
			// if the provided mount is a bind or rbind, it can't have opaque dirs
			// TODO:(sipsma) this will no longer be true once diffop is in place
			return nil
		}
	}
	if len(lowerdirs) == 1 {
		m = mount.Mount{
			Type:    "rbind",
			Source:  lowerdirs[0],
			Options: []string{"rbind", "ro"},
		}
	}

	var opaqueXattrName string
	if userxattr {
		opaqueXattrName = userOpaqueXattr
	} else {
		opaqueXattrName = trustedOpaqueXattr
	}

	return withTempMount(ctx, []mount.Mount{m}, func(lowerdir string) error {
		return filepath.WalkDir(fixupDir, func(path string, info fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				return nil
			}
			if _, err := unix.Lgetxattr(path, opaqueXattrName, make([]byte, 1)); err == nil {
				// TODO:
				// TODO:
				// TODO:
				// TODO:
				bklog.G(ctx).Debugf("found xattr %s on path %s", opaqueXattrName, path)

				relPath, err := filepath.Rel(fixupDir, path)
				if err != nil {
					return err
				}
				if _, err := os.Lstat(filepath.Join(lowerdir, relPath)); os.IsNotExist(err) {
					if err := unix.Lremovexattr(path, opaqueXattrName); err != nil {
						return err
					}
				}
				return fs.SkipDir // subdirs of an opaque dir can't also be opaque
			} else if filepath.Base(path) == "bar" {
				// TODO:
				// TODO:
				// TODO:
				// TODO:
				dirents, err := os.ReadDir(path)
				if err != nil {
					bklog.G(ctx).Errorf("failed to readdir %s: %v", path, err)
				} else {
					for _, de := range dirents {
						info, err := de.Info()
						if err != nil {
							bklog.G(ctx).Debugf("get info fail on %s: %v", path, err)
						}
						bklog.G(ctx).Debugf("listdir on %s: %+v %s", path, info, info.Mode())
					}
				}

			}
			return nil
		})
	})
}

type mergedOverlay struct {
	parents   []mount.Mount
	idmap     *idtools.IdentityMapping
	userxattr bool
	cleanup   func() error
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

		// TODO:(sipsma) optimize by de-duplicating options between mounts (most snapshotters don't return extra mount
		// options here, but if they did and the were duped, they would cause the 1-page size limit to be hit faster)
		c.options = append(c.options, b.options...)

		return c, nil
	}

	parse := func(mnt mount.Mount) (*overlay, error) {
		if mnt.Type != "overlay" && mnt.Type != "bind" {
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

	cleanupFuncs := []func() error{m.cleanup}
	doCleanup := func() error {
		var err error
		for i := range cleanupFuncs {
			f := cleanupFuncs[len(cleanupFuncs)-1-i]
			if f != nil {
				err = multierror.Append(err, f()).ErrorOrNil()
			}
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

	// De-dupe lowerdirs; if a lower appears more than once, then remove all occurrences besides the first one. This
	// is necessary for recent kernel overlay implementations that will return ELOOP if a lowerdir is repeated.
	// Additionally, if a lowerdir has no contents, leave it out as it can't have any impact on the overlay mount
	// except for bringing it closer to the max number of lowerdirs.
	var filteredLowers []string
	memo := map[string]struct{}{}
	if mm.upperdir != "" {
		// also dedupe any occurrences of the upperdir from lowerdirs
		memo[mm.upperdir] = struct{}{}
	}
	var emptyLowerdir string
	for _, lower := range mm.lowerdirs {
		if _, ok := memo[lower]; ok {
			continue
		}
		memo[lower] = struct{}{}
		if dirents, err := os.ReadDir(lower); err != nil {
			return nil, nil, errors.Wrap(err, "failed to read dirents of lowerdir during merge")
		} else if len(dirents) == 0 {
			emptyLowerdir = lower
			continue
		}
		filteredLowers = append(filteredLowers, lower)
	}
	mm.lowerdirs = filteredLowers
	if len(mm.lowerdirs) == 0 && mm.upperdir == "" && emptyLowerdir != "" {
		// If every lowerdir was empty and thus filtered out, we could end up with no lowerdirs or
		// an upperdir. Just use one of the empty lowerdirs in that case.
		mm.lowerdirs = []string{emptyLowerdir}
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
	if mm.upperdir != "" && mm.workdir == "" {
		return nil, nil, fmt.Errorf("invalid empty workdir when upperdir set")
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
