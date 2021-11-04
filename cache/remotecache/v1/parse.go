package cacheimport

import (
	"encoding/json"
	"strconv"

	"github.com/moby/buildkit/cache"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/contentutil"
	"github.com/pkg/errors"
)

func Parse(configJSON []byte, provider DescriptorProvider, t solver.CacheExporterTarget) error {
	var config CacheConfig
	if err := json.Unmarshal(configJSON, &config); err != nil {
		return errors.WithStack(err)
	}

	return ParseConfig(config, provider, t)
}

func ParseConfig(config CacheConfig, provider DescriptorProvider, t solver.CacheExporterTarget) error {
	cache := map[int]solver.CacheExporterRecord{}

	for i := range config.Records {
		if _, err := parseRecord(config, i, provider, t, cache); err != nil {
			return err
		}
	}
	return nil
}

func parseRecord(cc CacheConfig, idx int, provider DescriptorProvider, t solver.CacheExporterTarget, cache map[int]solver.CacheExporterRecord) (solver.CacheExporterRecord, error) {
	if r, ok := cache[idx]; ok {
		if r == nil {
			return nil, errors.Errorf("invalid looping record")
		}
		return r, nil
	}

	if idx < 0 || idx >= len(cc.Records) {
		return nil, errors.Errorf("invalid record ID: %d", idx)
	}
	rec := cc.Records[idx]

	r := t.Add(rec.Digest)
	cache[idx] = nil
	for i, inputs := range rec.Inputs {
		for _, inp := range inputs {
			src, err := parseRecord(cc, inp.LinkIndex, provider, t, cache)
			if err != nil {
				return nil, err
			}
			r.LinkFrom(src, i, inp.Selector)
		}
	}

	for _, res := range rec.Results {
		var remote *solver.Remote
		if rec.RecordType == MergeRecordType {
			// TODO: dedupe this with above maybe?
			// TODO: verify that order of inputs is always preserved
			var inputRemotes []*solver.Remote
			for _, inputs := range rec.Inputs {
				for _, inp := range inputs {
					// TODO: guard against panic when out of bounds
					inputRec := cc.Records[inp.LinkIndex]
					for _, inputRes := range inputRec.Results { // TODO: there's only ever 0 or 1 result, right?
						inputRemote, err := getRemoteChain(cc.Layers, inputRes.LayerIndexes, provider)
						if err != nil {
							return nil, err
						}
						inputRemotes = append(inputRemotes, inputRemote)
					}
				}
			}
			remote = mergeRemotes(identity.NewID(), inputRemotes)
		} else {
			// TODO: backwards compatibility
			// if res.LayerIndex != -1 {
			// 	fill in LayerIndexes
			// }

			// TODO: can you memoize anymore? visited := map[int]struct{}{}
			var err error
			remote, err = getRemoteChain(cc.Layers, res.LayerIndexes, provider)
			if err != nil {
				return nil, err
			}
		}
		r.AddResult(res.CreatedAt, remote)
	}

	cache[idx] = r
	return r, nil
}

func getRemoteChain(layers []CacheLayer, layerIndexes []int, provider DescriptorProvider) (*solver.Remote, error) {
	var r solver.Remote
	for _, idx := range layerIndexes {
		if idx < 0 || idx >= len(layers) {
			return nil, errors.Errorf("invalid layer index: %d", idx) // TODO: return nil, nil like other error case below?
		}
		l := layers[idx]

		descPair, ok := provider[l.Blob]
		if !ok {
			return nil, nil
		}

		r.Descriptors = append(r.Descriptors, descPair.Descriptor)
		mp := contentutil.NewMultiProvider(r.Provider)
		mp.Add(descPair.Descriptor.Digest, descPair.Provider)
		r.Provider = mp
	}
	return &r, nil
}

// mergeRemotes creates a remote the represents the merging of the given inputs
func mergeRemotes(mergeID string, inputs []*solver.Remote) *solver.Remote {
	var r solver.Remote
	for inputID, input := range inputs {
		for _, desc := range input.Descriptors {
			_, alreadyMerge := desc.Annotations[cache.MergeIDAnnotation]
			if alreadyMerge {
				continue
			}
			desc.Annotations[cache.MergeIDAnnotation] = mergeID
			desc.Annotations[cache.MergeInputIDAnnotation] = strconv.Itoa(inputID)
		}
	}
	return &r
}
