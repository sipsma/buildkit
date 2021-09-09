package snapshot

import (
	"context"
	"strings"
	"sync"

	"github.com/containerd/containerd/mount"
)

type Mounter interface {
	Mount() (string, error)
	Unmount() error
}

// LocalMounter is a helper for mounting mountfactory to temporary path. In
// addition it can mount binds without privileges
func LocalMounter(mountable Mountable) Mounter {
	return &localMounter{mountable: mountable}
}

// LocalMounterWithMounts is a helper for mounting to temporary path. In
// addition it can mount binds without privileges
func LocalMounterWithMounts(mounts []mount.Mount) Mounter {
	return &localMounter{mounts: mounts}
}

type localMounter struct {
	mu        sync.Mutex
	mounts    []mount.Mount
	mountable Mountable
	target    string
	release   func() error
}

// withTempMount is like mount.WithTempMount but avoids actually creating a mount if provided a bind-mount. This is
// useful for running in unit-tests and probably a very slight performance improvement but requires the callers respect
// any read-only flags as they will not be enforced by the bind-mount.
func withTempMount(ctx context.Context, mounts []mount.Mount, f func(root string) error) error {
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

// TODO: make this not ugly, dedupe with other code
func withApplyMount(ctx context.Context, mounts []mount.Mount, f func(root string) error) error {
	if len(mounts) == 1 {
		mnt := mounts[0]
		if mnt.Type == "overlay" {
			for _, opt := range mnt.Options {
				if strings.HasPrefix(opt, "upperdir=") {
					upperdir := strings.SplitN(opt, "=", 2)[1]
					return f(upperdir)
				}
			}
		}
	}
	return withTempMount(ctx, mounts, f)
}
