package cacheimport

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/bklog"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
)

func NewCacheChains() *CacheChains {
	return &CacheChains{visited: map[interface{}]struct{}{}}
}

type CacheChains struct {
	items   []*item
	visited map[interface{}]struct{}
}

func (c *CacheChains) Add(dgst digest.Digest) solver.CacheExporterRecord {
	if strings.HasPrefix(dgst.String(), "random:") {
		return &nopRecord{}
	}
	it := &item{c: c, dgst: dgst, backlinks: map[*item]struct{}{}}
	c.items = append(c.items, it)
	return it
}

func (c *CacheChains) Visit(v interface{}) {
	c.visited[v] = struct{}{}
}

func (c *CacheChains) Visited(v interface{}) bool {
	_, ok := c.visited[v]
	return ok
}

func (c *CacheChains) normalize() error {
	st := &normalizeState{
		added: map[*item]*item{},
		links: map[*item]map[nlink]map[digest.Digest]struct{}{},
		byKey: map[digest.Digest]*item{},
	}

	validated := make([]*item, 0, len(c.items))
	for _, it := range c.items {
		it.backlinksMu.Lock()
		it.validate()
		it.backlinksMu.Unlock()
	}
	for _, it := range c.items {
		if !it.invalid {
			validated = append(validated, it)
		} else {
			//TODO:
			//TODO:
			//TODO:
			//TODO:
			bklog.G(context.TODO()).Debugf("invalid item: %s", it.dgst)
		}
	}
	c.items = validated

	for _, it := range c.items {
		_, err := normalizeItem(it, st)
		if err != nil {
			return err
		}
	}

	st.removeLoops()

	items := make([]*item, 0, len(st.byKey))
	for _, it := range st.byKey {
		items = append(items, it)
	}
	c.items = items
	return nil
}

func (c *CacheChains) Marshal(ctx context.Context) (*CacheConfig, DescriptorProvider, error) {
	// TODO:
	// TODO:
	// TODO:
	// TODO:
	// TODO:
	/*
		var dbg []string
		for _, it := range c.items {
			dbg = append(dbg, fmt.Sprintf("%+v", it))
		}
		bklog.G(context.TODO()).Debugf("start marshal chain: %+v", dbg)
	*/

	if err := c.normalize(); err != nil {
		return nil, nil, err
	}

	/*
		// TODO:
		// TODO:
		// TODO:
		// TODO:
		// TODO:
		dbg = nil
		for _, it := range c.items {
			dbg = append(dbg, fmt.Sprintf("%+v", it))
		}
		bklog.G(context.TODO()).Debugf("normalized marshal chain: %+v", dbg)
	*/

	st := &marshalState{
		chainsByID:    map[digest.Digest][]int{},
		descriptors:   DescriptorProvider{},
		recordsByItem: map[*item]int{},
	}

	for _, it := range c.items {
		if err := marshalItem(ctx, it, st); err != nil {
			return nil, nil, err
		}
	}

	// TODO:
	// TODO:
	// TODO:
	// TODO:
	bklog.G(context.TODO()).Debugf("presort records: %+v", st.records)

	cc := CacheConfig{
		Layers:  st.layers,
		Records: st.records,
	}
	// TODO: update sortConfig and reenable
	// sortConfig(&cc)

	return &cc, st.descriptors, nil
}

type DescriptorProvider map[digest.Digest]DescriptorProviderPair

type DescriptorProviderPair struct {
	Descriptor ocispecs.Descriptor
	Provider   content.Provider
}

type item struct {
	c    *CacheChains
	dgst digest.Digest

	result     *solver.Remote
	resultTime time.Time

	// links is slice of sets of links to other items. The i'th set
	// in the slice holds the links from the i'th input.
	links []map[link]struct{}

	// backlinks is a set of items that have this item as an input.
	backlinksMu sync.Mutex
	backlinks   map[*item]struct{}

	invalid bool
}

type link struct {
	src      *item
	selector string
}

func (c *item) removeLink(src *item) bool {
	found := false
	for idx := range c.links {
		for l := range c.links[idx] {
			if l.src == src {
				delete(c.links[idx], l)
				found = true
			}
		}
	}
	for idx := range c.links {
		if len(c.links[idx]) == 0 {
			c.links = nil
			break
		}
	}
	return found
}

func (c *item) AddResult(createdAt time.Time, result *solver.Remote) {
	c.resultTime = createdAt
	c.result = result
}

func (c *item) LinkFrom(rec solver.CacheExporterRecord, index int, selector string) {
	src, ok := rec.(*item)
	if !ok {
		return
	}

	for {
		if index < len(c.links) {
			break
		}
		c.links = append(c.links, map[link]struct{}{})
	}

	c.links[index][link{src: src, selector: selector}] = struct{}{}
	src.backlinksMu.Lock()
	src.backlinks[c] = struct{}{}
	src.backlinksMu.Unlock()
}

func (c *item) validate() {
	for _, m := range c.links {
		if len(m) == 0 {
			c.invalid = true
			for bl := range c.backlinks {
				changed := false
				for _, m := range bl.links {
					for l := range m {
						if l.src == c {
							delete(m, l)
							changed = true
						}
					}
				}
				if changed {
					bl.validate()
				}
			}
			return
		}
	}
}

func (c *item) walkAllResults(fn func(i *item) error, visited map[*item]struct{}) error {
	if _, ok := visited[c]; ok {
		return nil
	}
	visited[c] = struct{}{}
	if err := fn(c); err != nil {
		return err
	}
	for _, links := range c.links {
		for l := range links {
			if err := l.src.walkAllResults(fn, visited); err != nil {
				return err
			}
		}
	}
	return nil
}

type nopRecord struct {
}

func (c *nopRecord) AddResult(createdAt time.Time, result *solver.Remote) {
}

func (c *nopRecord) LinkFrom(rec solver.CacheExporterRecord, index int, selector string) {
}

var _ solver.CacheExporterTarget = &CacheChains{}
