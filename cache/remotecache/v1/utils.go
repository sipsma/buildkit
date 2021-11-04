package cacheimport

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/moby/buildkit/cache"
	"github.com/moby/buildkit/exporter/containerimage/exptypes"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/bklog"
	digest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// EmptyLayerRemovalSupported defines if implementation supports removal of empty layers. Buildkit image exporter
// removes empty layers, but moby layerstore based implementation does not.
var EmptyLayerRemovalSupported = true

// sortConfig sorts the config structure to make sure it is deterministic
// TODO: fix to use LayerIndexes
func sortConfig(cc *CacheConfig) {
	type indexedLayer struct {
		oldIndex int
		newIndex int
		l        CacheLayer
	}

	unsortedLayers := make([]*indexedLayer, len(cc.Layers))
	sortedLayers := make([]*indexedLayer, len(cc.Layers))

	for i, l := range cc.Layers {
		il := &indexedLayer{oldIndex: i, l: l}
		unsortedLayers[i] = il
		sortedLayers[i] = il
	}
	sort.Slice(sortedLayers, func(i, j int) bool {
		li := sortedLayers[i].l
		lj := sortedLayers[j].l
		if li.Blob == lj.Blob {
			return li.ParentIndex < lj.ParentIndex
		}
		return li.Blob < lj.Blob
	})
	for i, l := range sortedLayers {
		l.newIndex = i
	}

	layers := make([]CacheLayer, len(sortedLayers))
	for i, l := range sortedLayers {
		if pID := l.l.ParentIndex; pID != -1 {
			l.l.ParentIndex = unsortedLayers[pID].newIndex
		}
		layers[i] = l.l
	}

	type indexedRecord struct {
		oldIndex int
		newIndex int
		r        CacheRecord
	}

	unsortedRecords := make([]*indexedRecord, len(cc.Records))
	sortedRecords := make([]*indexedRecord, len(cc.Records))

	for i, r := range cc.Records {
		ir := &indexedRecord{oldIndex: i, r: r}
		unsortedRecords[i] = ir
		sortedRecords[i] = ir
	}
	sort.Slice(sortedRecords, func(i, j int) bool {
		ri := sortedRecords[i].r
		rj := sortedRecords[j].r
		if ri.Digest != rj.Digest {
			return ri.Digest < rj.Digest
		}
		if len(ri.Inputs) != len(rj.Inputs) {
			return len(ri.Inputs) < len(rj.Inputs)
		}
		for i, inputs := range ri.Inputs {
			if len(ri.Inputs[i]) != len(rj.Inputs[i]) {
				return len(ri.Inputs[i]) < len(rj.Inputs[i])
			}
			for j := range inputs {
				if ri.Inputs[i][j].Selector != rj.Inputs[i][j].Selector {
					return ri.Inputs[i][j].Selector < rj.Inputs[i][j].Selector
				}
				inputDigesti := cc.Records[ri.Inputs[i][j].LinkIndex].Digest
				inputDigestj := cc.Records[rj.Inputs[i][j].LinkIndex].Digest
				if inputDigesti != inputDigestj {
					return inputDigesti < inputDigestj
				}
			}
		}
		return false
	})
	for i, l := range sortedRecords {
		l.newIndex = i
	}

	records := make([]CacheRecord, len(sortedRecords))
	for i, r := range sortedRecords {
		for j := range r.r.Results {
			r.r.Results[j].LayerIndex = unsortedLayers[r.r.Results[j].LayerIndex].newIndex
		}
		for j, inputs := range r.r.Inputs {
			for k := range inputs {
				r.r.Inputs[j][k].LinkIndex = unsortedRecords[r.r.Inputs[j][k].LinkIndex].newIndex
			}
			sort.Slice(inputs, func(i, j int) bool {
				return inputs[i].LinkIndex < inputs[j].LinkIndex
			})
		}
		records[i] = r.r
	}

	cc.Layers = layers
	cc.Records = records
}

func outputKey(dgst digest.Digest, idx int) digest.Digest {
	return digest.FromBytes([]byte(fmt.Sprintf("%s@%d", dgst, idx)))
}

type nlink struct {
	dgst     digest.Digest
	input    int
	selector string
}
type normalizeState struct {
	added map[*item]*item
	links map[*item]map[nlink]map[digest.Digest]struct{}
	byKey map[digest.Digest]*item
	next  int
}

func (s *normalizeState) removeLoops() {
	roots := []digest.Digest{}
	for dgst, it := range s.byKey {
		if len(it.links) == 0 {
			roots = append(roots, dgst)
		}
	}

	visited := map[digest.Digest]struct{}{}

	for _, d := range roots {
		s.checkLoops(d, visited)
	}
}

func (s *normalizeState) checkLoops(d digest.Digest, visited map[digest.Digest]struct{}) {
	it, ok := s.byKey[d]
	if !ok {
		return
	}
	links, ok := s.links[it]
	if !ok {
		return
	}
	visited[d] = struct{}{}
	defer func() {
		delete(visited, d)
	}()

	for l, ids := range links {
		for id := range ids {
			if _, ok := visited[id]; ok {
				it2, ok := s.byKey[id]
				if !ok {
					continue
				}
				if !it2.removeLink(it) {
					logrus.Warnf("failed to remove looping cache key %s %s", d, id)
				}
				delete(links[l], id)
			} else {
				s.checkLoops(id, visited)
			}
		}
	}
}

// normalizeItem identifies any equivalent items (where equivalence is determined
// by comparing the hash of the item and the identity+order of its inputs).
// Equivalent items are de-duped, with links updated to point to a single instance
// of the duplicates.
func normalizeItem(it *item, state *normalizeState) (*item, error) {
	if it2, ok := state.added[it]; ok {
		return it2, nil
	}

	if len(it.links) == 0 {
		id := it.dgst
		if it2, ok := state.byKey[id]; ok {
			state.added[it] = it2
			return it2, nil
		}
		state.byKey[id] = it
		state.added[it] = it
		return nil, nil
	}

	matches := map[digest.Digest]struct{}{}

	// check if there is already a matching record
	for i, m := range it.links {
		if len(m) == 0 {
			return nil, errors.Errorf("invalid incomplete links")
		}
		for l := range m {
			nl := nlink{dgst: it.dgst, input: i, selector: l.selector}
			it2, err := normalizeItem(l.src, state)
			if err != nil {
				return nil, err
			}
			links := state.links[it2][nl]
			if i == 0 {
				for id := range links {
					matches[id] = struct{}{}
				}
			} else {
				for id := range matches {
					if _, ok := links[id]; !ok {
						delete(matches, id)
					}
				}
			}
		}
	}

	var id digest.Digest

	links := it.links

	if len(matches) > 0 {
		for m := range matches {
			if id == "" || id > m {
				id = m
			}
		}
	} else {
		// keep tmp IDs deterministic
		state.next++
		id = digest.FromBytes([]byte(fmt.Sprintf("%d", state.next)))
		state.byKey[id] = it
		it.links = make([]map[link]struct{}, len(it.links))
		for i := range it.links {
			it.links[i] = map[link]struct{}{}
		}
	}

	it2 := state.byKey[id]
	state.added[it] = it2

	for i, m := range links {
		for l := range m {
			subIt, err := normalizeItem(l.src, state)
			if err != nil {
				return nil, err
			}
			it2.links[i][link{src: subIt, selector: l.selector}] = struct{}{}

			nl := nlink{dgst: it.dgst, input: i, selector: l.selector}
			if _, ok := state.links[subIt]; !ok {
				state.links[subIt] = map[nlink]map[digest.Digest]struct{}{}
			}
			if _, ok := state.links[subIt][nl]; !ok {
				state.links[subIt][nl] = map[digest.Digest]struct{}{}
			}
			state.links[subIt][nl][id] = struct{}{}
		}
	}

	return it2, nil
}

// getSubremotes returns the remotes representing each merge input
// of the provided remote. If the provided remote is not a merge, then
// it is returned unaltered as the only element of the slice.
func getSubremotes(r *solver.Remote) []solver.Remote {
	if r == nil {
		return nil
	}
	topDesc := r.Descriptors[len(r.Descriptors)-1]
	mergeID, isMerge := topDesc.Annotations[cache.MergeIDAnnotation]
	if !isMerge {
		return []solver.Remote{*r}
	}

	maxInputID, err := strconv.Atoi(topDesc.Annotations[cache.MergeInputIDAnnotation])
	if err != nil {
		return []solver.Remote{*r} // TODO: should this be an error?
	}

	subremotes := make([]solver.Remote, maxInputID+1)
	curInput := 0
	for _, desc := range r.Descriptors {
		if desc.Annotations[cache.MergeIDAnnotation] != mergeID {
			subremotes[curInput].Descriptors = append(subremotes[curInput].Descriptors, desc)
			continue
		}
		input, err := strconv.Atoi(desc.Annotations[cache.MergeInputIDAnnotation])
		if err != nil {
			return []solver.Remote{*r} // TODO: should this be an error?
		}
		if input != curInput {
			curInput++
		}
		delete(desc.Annotations, cache.MergeIDAnnotation)
		delete(desc.Annotations, cache.MergeInputIDAnnotation)
		subremotes[curInput].Descriptors = append(subremotes[curInput].Descriptors, desc)
	}
	return subremotes
}

type marshalState struct {
	layers      []CacheLayer
	chainsByID  map[digest.Digest][]int // map of remote digest to layer indexes TODO: n^2
	descriptors DescriptorProvider

	records       []CacheRecord
	recordsByItem map[*item]int
}

func getRemoteDigest(r *solver.Remote) digest.Digest {
	var digests []digest.Digest
	for _, desc := range r.Descriptors {
		digests = append(digests, desc.Digest)
	}
	return identity.ChainID(digests)
}

// TODO: doc
func marshalRemote(ctx context.Context, r *solver.Remote, state *marshalState) []int {
	if len(r.Descriptors) == 0 {
		return nil
	}

	if cd, ok := r.Provider.(interface {
		CheckDescriptor(context.Context, ocispecs.Descriptor) error
	}); ok && len(r.Descriptors) > 0 {
		for _, d := range r.Descriptors {
			if err := cd.CheckDescriptor(ctx, d); err != nil {
				return nil
			}
		}
	}

	desc := r.Descriptors[len(r.Descriptors)-1]

	if _, isMerge := desc.Annotations[cache.MergeIDAnnotation]; isMerge {
		var layerIndexes []int
		subremotes := getSubremotes(r)
		for _, subremote := range subremotes {
			subLayerIndexes := marshalRemote(ctx, &subremote, state)
			layerIndexes = append(layerIndexes, subLayerIndexes...)
		}
		return layerIndexes
	}

	// TODO: n^2
	// id := desc.Digest.String() + parentID
	id := getRemoteDigest(r)
	if cached, ok := state.chainsByID[id]; ok {
		return cached
	}

	var parentLayerIndexes []int
	if len(r.Descriptors) > 1 {
		r2 := &solver.Remote{
			Descriptors: r.Descriptors[:len(r.Descriptors)-1],
			Provider:    r.Provider,
		}
		parentLayerIndexes = marshalRemote(ctx, r2, state)
	}

	if desc.Digest == exptypes.EmptyGZLayer && EmptyLayerRemovalSupported {
		return parentLayerIndexes
	}

	state.descriptors[desc.Digest] = DescriptorProviderPair{
		Descriptor: desc,
		Provider:   r.Provider,
	}

	l := CacheLayer{
		Blob:        desc.Digest,
		ParentIndex: -1,
	}
	if len(parentLayerIndexes) > 0 {
		l.ParentIndex = parentLayerIndexes[len(parentLayerIndexes)-1]
	}
	newLayerIndex := len(state.layers)
	state.layers = append(state.layers, l)

	state.chainsByID[id] = append(state.chainsByID[id], parentLayerIndexes...)
	state.chainsByID[id] = append(state.chainsByID[id], newLayerIndex)
	return state.chainsByID[id]
}

func marshalItem(ctx context.Context, it *item, state *marshalState) error {
	if _, ok := state.recordsByItem[it]; ok {
		return nil
	}

	rec := CacheRecord{
		Digest: it.dgst,
		Inputs: make([][]CacheInput, len(it.links)),
	}

	for i, m := range it.links {
		for l := range m {
			if err := marshalItem(ctx, l.src, state); err != nil {
				return err
			}
			idx, ok := state.recordsByItem[l.src]
			if !ok {
				return errors.Errorf("invalid source record: %v", l.src)
			}
			rec.Inputs[i] = append(rec.Inputs[i], CacheInput{
				Selector:  l.selector,
				LinkIndex: idx,
			})
		}
	}

	if it.result != nil {
		if layerIndexes := marshalRemote(ctx, it.result, state); len(layerIndexes) > 0 {
			rec.Results = append(rec.Results, CacheResult{
				LayerIndexes: layerIndexes,
				CreatedAt:    it.resultTime,
			})
			// TODO: util func
			_, isMerge := it.result.Descriptors[len(it.result.Descriptors)-1].Annotations[cache.MergeIDAnnotation]
			if isMerge {
				rec.RecordType = MergeRecordType
			}
		}
	}

	state.recordsByItem[it] = len(state.records)
	state.records = append(state.records, rec)

	// TODO:
	// TODO:
	// TODO:
	// TODO:
	// TODO:
	// fmt.Printf("%+v %+v\n\n", rec, errors.New("fjdklas"))
	// bklog.G(ctx).Debugf("%+v %+v\n", rec, errors.New("fjdklas"))
	bklog.G(ctx).Debugf("%+v", state.records)

	return nil
}

func isSubRemote(sub, main solver.Remote) bool {
	if len(sub.Descriptors) > len(main.Descriptors) {
		return false
	}
	for i := range sub.Descriptors {
		if sub.Descriptors[i].Digest != main.Descriptors[i].Digest {
			return false
		}
	}
	return true
}
