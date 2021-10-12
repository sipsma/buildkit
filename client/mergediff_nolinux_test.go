//go:build !linux
// +build !linux

package client

import (
	"os"

	"github.com/containerd/continuity/fs/fstest"
	"github.com/pkg/errors"
)

func mknod(path string, mode os.FileMode, maj, min uint32) fstest.Applier {
	return applyFn(func(root string) error {
		return errors.New("mknod not supported on this platform")
	})
}
