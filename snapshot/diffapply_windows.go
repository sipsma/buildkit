// +build windows

package snapshot

import (
	"context"

	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/pkg/errors"
)

func diffApply(ctx context.Context, lowerMounts, upperMounts, applyMounts []mount.Mount, useHardlink bool, externalHardlinks map[uint64]struct{}) error {
	return errors.New("diff apply not supported on windows")
}

func diskUsage(ctx context.Context, mounts []mount.Mount, externalHardlinks map[uint64]struct{}) (snapshots.Usage, error) {
	return snapshots.Usage{}, errors.New("disk usage not supported on windows")
}
