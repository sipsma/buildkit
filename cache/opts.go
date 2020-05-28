package cache

import (
	"github.com/containerd/containerd/content"
	"github.com/moby/buildkit/util/cacheutil"
	"github.com/moby/buildkit/util/progress"
	digest "github.com/opencontainers/go-digest"
)

type descHandler struct {
	count    int64
	Provider content.Provider
	ImageRef string
	progress.Callbacks
}

type DescHandlerSet interface {
	Get(digest.Digest) *descHandler
}

type descHandlerKey digest.Digest

func (descHandlerKey) OptSetKey() {}

type descHandlerSet struct {
	cacheutil.OptSet
}

func (s descHandlerSet) Get(k digest.Digest) *descHandler {
	if v, ok := s.Value(descHandlerKey(k)).(*descHandler); ok {
		return v
	}
	return nil
}

func AsDescHandlerSet(optSet cacheutil.OptSet) DescHandlerSet {
	return descHandlerSet{OptSet: optSet}
}

func AsOptSet(r *Remote, pc progress.Callbacks, imageRef string) cacheutil.OptSet {
	descHandler := &descHandler{
		Provider:  r.Provider,
		ImageRef:  imageRef,
		Callbacks: pc,
	}
	m := cacheutil.OptSetMap(make(map[interface{}]interface{}))
	for _, desc := range r.Descriptors {
		m[descHandlerKey(desc.Digest)] = descHandler
	}
	return m
}
