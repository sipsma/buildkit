package pull

import (
	"context"
	"sync"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/reference"
	"github.com/containerd/containerd/remotes"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/containerd/containerd/remotes/docker/schema1"
	workercache "github.com/moby/buildkit/cache"
	"github.com/moby/buildkit/util/contentutil"
	"github.com/moby/buildkit/util/flightcontrol"
	"github.com/moby/buildkit/util/imageutil"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

type Puller struct {
	ContentStore content.Store
	Resolver     remotes.Resolver
	Src          reference.Spec
	Platform     *ocispec.Platform
	G            *flightcontrol.Group

	resolveOnce sync.Once
	resolveErr  error
	fetcher     remotes.Fetcher
	desc        ocispec.Descriptor
	ref         string
	layers      []ocispec.Descriptor
	nonlayers   []ocispec.Descriptor
}

var _ content.Provider = &Puller{}

type PulledMetadata struct {
	Ref              string
	MainManifestDesc ocispec.Descriptor
	Nonlayers        []ocispec.Descriptor
	Remote           *workercache.Remote
}

func (p *Puller) resolve(ctx context.Context) error {
	p.resolveOnce.Do(func() {
		desc := ocispec.Descriptor{
			Digest: p.Src.Digest(),
		}

		if desc.Digest != "" && func() error {
			info, err := p.ContentStore.Info(ctx, desc.Digest)
			if err != nil {
				return err
			}
			desc.Size = info.Size
			p.ref = p.Src.String()
			fetcher, err := p.Resolver.Fetcher(ctx, p.ref)
			if err != nil {
				return err
			}
			p.fetcher = fetcher
			ra, err := p.ContentStore.ReaderAt(ctx, desc)
			if err != nil {
				return err
			}
			mt, err := imageutil.DetectManifestMediaType(ra)
			if err != nil {
				return err
			}
			desc.MediaType = mt
			p.desc = desc
			return nil
		}() == nil {
			return
		}

		ref, desc, err := p.Resolver.Resolve(ctx, p.Src.String())
		if err != nil {
			p.resolveErr = err
			return
		}
		p.desc = desc
		p.ref = ref

		fetcher, err := p.Resolver.Fetcher(ctx, p.ref)
		if err != nil {
			p.resolveErr = err
			return
		}
		p.fetcher = fetcher
	})

	return p.resolveErr
}

func (p *Puller) PullMetadata(ctx context.Context) (*PulledMetadata, error) {
	err := p.resolve(ctx)
	if err != nil {
		return nil, err
	}

	_, err = p.G.Do(ctx, p.desc.Digest.String(), func(ctx context.Context) (interface{}, error) {
		// workaround for gcr, authentication not supported on blob endpoints
		EnsureManifestRequested(ctx, p.Resolver, p.ref)

		var platform platforms.MatchComparer
		if p.Platform != nil {
			platform = platforms.Only(*p.Platform)
		} else {
			platform = platforms.Default()
		}

		var mu sync.Mutex // images.Dispatch calls handlers in parallel
		metadata := make(map[digest.Digest]ocispec.Descriptor)

		// TODO: need a wrapper snapshot interface that combines content
		// and snapshots as 1) buildkit shouldn't have a dependency on contentstore
		// or 2) cachemanager should manage the contentstore
		var handlers []images.Handler

		var schema1Converter *schema1.Converter
		if p.desc.MediaType == images.MediaTypeDockerSchema1Manifest {
			schema1Converter = schema1.NewConverter(p.ContentStore, p.fetcher)
			handlers = append(handlers, schema1Converter)
		} else {
			// Get all the children for a descriptor
			childrenHandler := images.ChildrenHandler(p.ContentStore)
			// Filter the children by the platform
			childrenHandler = images.FilterPlatforms(childrenHandler, platform)
			// Limit manifests pulled to the best match in an index
			childrenHandler = images.LimitManifests(childrenHandler, platform, 1)

			dslHandler, err := docker.AppendDistributionSourceLabel(p.ContentStore, p.ref)
			if err != nil {
				return nil, err
			}
			handlers = append(handlers,
				filterLayerBlobs(metadata, &mu),
				remotes.FetchHandler(p.ContentStore, p.fetcher),
				childrenHandler,
				dslHandler,
			)
		}

		if err := images.Dispatch(ctx, images.Handlers(handlers...), nil, p.desc); err != nil {
			return nil, err
		}

		if schema1Converter != nil {
			p.desc, err = schema1Converter.Convert(ctx)
			if err != nil {
				return nil, err
			}

			handlers := []images.Handler{
				filterLayerBlobs(metadata, &mu),
				images.FilterPlatforms(images.ChildrenHandler(p.ContentStore), platform),
			}

			if err := images.Dispatch(ctx, images.Handlers(handlers...), nil, p.desc); err != nil {
				return nil, err
			}
		}

		for _, desc := range metadata {
			p.nonlayers = append(p.nonlayers, desc)
		}

		// split all pulled data to layers and rest. layers remain roots and are deleted with snapshots. rest will be linked to layers.
		p.layers, err = getLayers(ctx, p.ContentStore, p.desc, platform)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		return nil, err
	}

	return &PulledMetadata{
		Ref:              p.ref,
		MainManifestDesc: p.desc,
		Nonlayers:        p.nonlayers,
		Remote: &workercache.Remote{
			Descriptors: p.layers,
			Provider:    p,
		},
	}, nil
}

func (p *Puller) ReaderAt(ctx context.Context, desc ocispec.Descriptor) (content.ReaderAt, error) {
	err := p.resolve(ctx)
	if err != nil {
		return nil, err
	}

	return contentutil.FromFetcher(p.fetcher).ReaderAt(ctx, desc)
}

func filterLayerBlobs(metadata map[digest.Digest]ocispec.Descriptor, mu sync.Locker) images.HandlerFunc {
	return func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		switch desc.MediaType {
		case ocispec.MediaTypeImageLayer, images.MediaTypeDockerSchema2Layer, ocispec.MediaTypeImageLayerGzip, images.MediaTypeDockerSchema2LayerGzip, images.MediaTypeDockerSchema2LayerForeign, images.MediaTypeDockerSchema2LayerForeignGzip:
			return nil, images.ErrSkipDesc
		default:
			if metadata != nil {
				mu.Lock()
				metadata[desc.Digest] = desc
				mu.Unlock()
			}
		}
		return nil, nil
	}
}

func getLayers(ctx context.Context, provider content.Provider, desc ocispec.Descriptor, platform platforms.MatchComparer) ([]ocispec.Descriptor, error) {
	manifest, err := images.Manifest(ctx, provider, desc, platform)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	image := images.Image{Target: desc}
	diffIDs, err := image.RootFS(ctx, provider, platform)
	if err != nil {
		return nil, errors.Wrap(err, "failed to resolve rootfs")
	}
	if len(diffIDs) != len(manifest.Layers) {
		return nil, errors.Errorf("mismatched image rootfs and manifest layers %+v %+v", diffIDs, manifest.Layers)
	}
	layers := make([]ocispec.Descriptor, len(diffIDs))
	for i := range diffIDs {
		desc := manifest.Layers[i]
		if desc.Annotations == nil {
			desc.Annotations = map[string]string{}
		}
		desc.Annotations["containerd.io/uncompressed"] = diffIDs[i].String()
		layers[i] = desc
	}
	return layers, nil
}
