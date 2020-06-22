package cache

import (
	"fmt"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/moby/buildkit/util/progress"
	digest "github.com/opencontainers/go-digest"
)

type DescHandlerKey digest.Digest

type DescHandler struct {
	count          int64
	started        *time.Time
	Provider       content.Provider
	ImageRef       string
	ProgressWriter progress.Writer
	VertexDigest   digest.Digest
	VertexName     string
}

type MissingDescHandler DescHandlerKey

func (m MissingDescHandler) Error() string {
	return fmt.Sprintf("missing descriptor handler for lazy blob %q", digest.Digest(m))
}

func (m MissingDescHandler) FindIn(getter func(interface{}) interface{}) *DescHandler {
	if getter == nil {
		return nil
	}
	if v, ok := getter(DescHandlerKey(m)).(*DescHandler); ok {
		return v
	}
	return nil
}

func descHandlerOf(opts ...RefOption) *DescHandler {
	for _, opt := range opts {
		if opt, ok := opt.(*DescHandler); ok {
			return opt
		}
	}
	return nil
}
