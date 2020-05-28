package cacheutil

import (
	"context"
)

type OptSet interface {
	Value(interface{}) interface{}
}

var _ OptSet = context.Context(nil)

type OptSetKey interface {
	// OptSetKey is a no-op method that just marks a type as one
	// meant to use as a key to a cache opt set, allowing callers
	// to identify them as such when behind an opaque interface{}
	OptSetKey()
}

type OptSetMap map[interface{}]interface{}

var _ OptSet = OptSetMap(nil)

func (m OptSetMap) Value(k interface{}) interface{} {
	return m[k]
}

func WithOptSet(ctx context.Context, optSet OptSet) context.Context {
	return ctxWithOptSet{Context: ctx, optSet: optSet}
}

type ctxWithOptSet struct {
	context.Context
	optSet OptSet
}

func (c ctxWithOptSet) Value(k interface{}) interface{} {
	if v := c.optSet.Value(k); v != nil {
		return v
	}
	return c.Context.Value(k)
}
