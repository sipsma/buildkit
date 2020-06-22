package solver

import (
	"context"

	digest "github.com/opencontainers/go-digest"
	"github.com/sirupsen/logrus"
)

type CacheOpts map[interface{}]interface{}

type CacheOptGetter func(key interface{}) interface{} // TODO this type definition isn't very helpful
type CacheOptGetterKey struct{}

func CacheOptGetterOf(ctx context.Context) CacheOptGetter {
	if v := ctx.Value(CacheOptGetterKey{}); v != nil {
		if getter, ok := v.(CacheOptGetter); ok {
			return getter
		}
	}
	return nil
}

func withAncestorCacheOpts(ctx context.Context, start *state) context.Context {
	return context.WithValue(ctx, CacheOptGetterKey{}, CacheOptGetter(func(k interface{}) (v interface{}) {
		walkAncestors(start, func(st *state) bool {
			if st.clientVertex.Error != "" {
				// don't use values from cancelled or otherwise error'd vertexes
				return false
			}
			for _, res := range st.op.cacheRes {
				if res.Opts == nil {
					continue
				}
				var ok bool
				if v, ok = res.Opts[k]; ok {
					return true
				}
			}
			return false
		})
		return v
	}))
}

func walkAncestors(start *state, f func(*state) bool) {
	stack := [][]*state{{start}}
	cache := make(map[digest.Digest]struct{})
	for len(stack) > 0 {
		sts := stack[len(stack)-1]
		if len(sts) == 0 {
			stack = stack[:len(stack)-1]
			continue
		}
		st := sts[len(sts)-1]
		stack[len(stack)-1] = sts[:len(sts)-1]
		if st == nil {
			continue
		}
		if _, ok := cache[st.origDigest]; ok {
			continue
		}
		cache[st.origDigest] = struct{}{}
		if shouldStop := f(st); shouldStop {
			return
		}
		stack = append(stack, []*state{})
		for _, parentDgst := range st.clientVertex.Inputs {
			st.solver.mu.RLock()
			parent := st.solver.actives[parentDgst]
			st.solver.mu.RUnlock()
			if parent == nil {
				logrus.Warnf("parent %q not found in active job list during cache opt search", parentDgst)
				continue
			}
			stack[len(stack)-1] = append(stack[len(stack)-1], parent)
		}
	}
}
