package solver

import (
	"context"

	"github.com/moby/buildkit/util/cacheutil"
	digest "github.com/opencontainers/go-digest"
	"github.com/sirupsen/logrus"
)

func withAncestorCacheOpts(ctx context.Context, start *state) context.Context {
	return ancestorCacheOpts{
		Context: ctx,
		start:   start,
	}
}

type ancestorCacheOpts struct {
	context.Context
	start *state
}

func (c ancestorCacheOpts) Value(k interface{}) (v interface{}) {
	if v = c.Context.Value(k); v != nil {
		return
	}

	if _, ok := k.(cacheutil.OptSetKey); !ok {
		// if this isn't an opt set key, don't spend time
		// recursing to the whole build graph
		return nil
	}

	walkAncestors(c.start, func(st *state) bool {
		if st.clientVertex.Error != "" {
			// don't use values from cancelled or otherwise error'd vertexes
			return false
		}
		for _, res := range st.op.cacheRes {
			if res.CacheOpts == nil {
				continue
			}
			if v = res.CacheOpts.Value(k); v != nil {
				return true
			}
		}
		return false
	})
	return
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

func withJobCacheOpts(ctx context.Context, j *Job) context.Context {
	return jobCacheOpts{Context: ctx, job: j}
}

type jobCacheOpts struct {
	context.Context
	job *Job
}

func (c jobCacheOpts) Value(k interface{}) interface{} {
	if v := c.Context.Value(k); v != nil {
		return v
	}

	var actives []*state
	c.job.list.mu.RLock()
	for _, st := range c.job.list.actives {
		actives = append(actives, st)
	}
	c.job.list.mu.RUnlock()
	for _, st := range actives {
		if _, ok := st.jobs[c.job]; !ok {
			continue
		}
		if st.op == nil {
			continue
		}
		for _, res := range st.op.cacheRes {
			if res.CacheOpts == nil {
				continue
			}
			if v := res.CacheOpts.Value(k); v != nil {
				return v
			}
		}
	}
	return nil
}
