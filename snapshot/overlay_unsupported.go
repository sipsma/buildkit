// +build !linux

package snapshot

import (
	"context"
	"runtime"

	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/pkg/errors"
)

func needsUserXAttr(ctx context.Context, sn snapshots.Snapshotter, lm leases.Manager) (bool, error) {
	return false, errors.New("checking userxattr unsupported on " + runtime.GOOS)
}

func fixOpaqueDirs(ctx context.Context, mounts []mount.Mount, userxattr bool) error {
	return errors.New("fixing opaque dirs unsupported on " + runtime.GOOS)
}

func (m mergedOverlay) Mount() (rmnts []mount.Mount, _ func() error, rerr error) {
	return nil, nil, errors.New("merging overlay mounts unsupported on " + runtime.GOOS)
}
