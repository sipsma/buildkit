package snapshot

import (
	"github.com/containerd/containerd/mount"
	"github.com/docker/docker/pkg/idtools"
)

type mergedOverlay struct {
	parents   []mount.Mount
	idmap     *idtools.IdentityMapping
	userxattr bool
	cleanup   func() error
}

func (m mergedOverlay) IdentityMapping() *idtools.IdentityMapping {
	return m.idmap
}
