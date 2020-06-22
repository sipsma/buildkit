package containerimage

import (
	"context"
	"encoding/json"
	"runtime"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/diff"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/docker/distribution/reference"
	"github.com/moby/buildkit/cache"
	"github.com/moby/buildkit/client/llb"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/snapshot"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/source"
	"github.com/moby/buildkit/util/flightcontrol"
	"github.com/moby/buildkit/util/imageutil"
	"github.com/moby/buildkit/util/leaseutil"
	"github.com/moby/buildkit/util/progress"
	"github.com/moby/buildkit/util/pull"
	digest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

// TODO: break apart containerd specifics like contentstore so the resolver
// code can be used with any implementation

type SourceOpt struct {
	Snapshotter   snapshot.Snapshotter
	ContentStore  content.Store
	Applier       diff.Applier
	CacheAccessor cache.Accessor
	ImageStore    images.Store // optional
	RegistryHosts docker.RegistryHosts
	LeaseManager  leases.Manager
}

type imageSource struct {
	SourceOpt
	g flightcontrol.Group
}

func NewSource(opt SourceOpt) (source.Source, error) {
	is := &imageSource{
		SourceOpt: opt,
	}

	return is, nil
}

func (is *imageSource) ID() string {
	return source.DockerImageScheme
}

func (is *imageSource) ResolveImageConfig(ctx context.Context, ref string, opt llb.ResolveImageConfigOpt, sm *session.Manager) (digest.Digest, []byte, error) {
	type t struct {
		dgst digest.Digest
		dt   []byte
	}
	key := ref
	if platform := opt.Platform; platform != nil {
		key += platforms.Format(*platform)
	}

	rm, err := source.ParseImageResolveMode(opt.ResolveMode)
	if err != nil {
		return "", nil, err
	}

	res, err := is.g.Do(ctx, key, func(ctx context.Context) (interface{}, error) {
		dgst, dt, err := imageutil.Config(ctx, ref, pull.NewResolver(ctx, is.RegistryHosts, sm, is.ImageStore, rm, ref), is.ContentStore, is.LeaseManager, opt.Platform)
		if err != nil {
			return nil, err
		}
		return &t{dgst: dgst, dt: dt}, nil
	})
	if err != nil {
		return "", nil, err
	}
	typed := res.(*t)
	return typed.dgst, typed.dt, nil
}

func (is *imageSource) Resolve(ctx context.Context, id source.Identifier, sm *session.Manager, vtx solver.Vertex) (source.SourceInstance, error) {
	imageIdentifier, ok := id.(*source.ImageIdentifier)
	if !ok {
		return nil, errors.Errorf("invalid image identifier %v", id)
	}

	platform := platforms.DefaultSpec()
	if imageIdentifier.Platform != nil {
		platform = *imageIdentifier.Platform
	}

	resolver := pull.NewResolver(ctx, is.RegistryHosts, sm, is.ImageStore, imageIdentifier.ResolveMode, imageIdentifier.Reference.String())

	pullerUtil := &pull.Puller{
		ContentStore: is.ContentStore,
		Resolver:     resolver,
		Platform:     &platform,
		Src:          imageIdentifier.Reference,
		G:            &is.g,
	}
	p := &puller{
		CacheAccessor: is.CacheAccessor,
		LeaseManager:  is.LeaseManager,
		Puller:        pullerUtil,
		id:            imageIdentifier,
		vtx:           vtx,
	}
	return p, nil
}

type puller struct {
	CacheAccessor    cache.Accessor
	LeaseManager     leases.Manager
	id               *source.ImageIdentifier
	vtx              solver.Vertex
	descHandler      *cache.DescHandler
	releaseTmpLeases func(context.Context) error
	*pull.Puller
}

func mainManifestKey(ctx context.Context, desc specs.Descriptor, platform *specs.Platform) (digest.Digest, error) {
	keyStruct := struct {
		Digest  digest.Digest
		OS      string
		Arch    string
		Variant string `json:",omitempty"`
	}{
		Digest: desc.Digest,
	}
	if platform != nil {
		keyStruct.OS = platform.OS
		keyStruct.Arch = platform.Architecture
		keyStruct.Variant = platform.Variant
	}

	dt, err := json.Marshal(keyStruct)
	if err != nil {
		return "", err
	}
	return digest.FromBytes(dt), nil
}

func (p *puller) CacheKey(ctx context.Context, index int) (_ string, cacheOpts solver.CacheOpts, _ bool, err error) {
	ctx, done, err := leaseutil.WithLease(ctx, p.LeaseManager, leases.WithExpiration(5*time.Minute), leaseutil.MakeTemporary)
	if err != nil {
		return "", nil, false, err
	}
	p.releaseTmpLeases = done
	imageutil.AddLease(p.releaseTmpLeases)
	defer func() {
		if err != nil {
			p.releaseTmpLeases(ctx)
		}
	}()

	resolveProgressDone := oneOffProgress(ctx, "resolve "+p.Src.String())
	defer func() {
		resolveProgressDone(err)
	}()

	metadata, err := p.PullMetadata(ctx)
	if err != nil {
		return "", nil, false, err
	}

	if len(metadata.Remote.Descriptors) > 0 {
		topDesc := metadata.Remote.Descriptors[len(metadata.Remote.Descriptors)-1]
		pw, _, _ := progress.FromContext(ctx)
		p.descHandler = &cache.DescHandler{
			Provider:       metadata.Remote.Provider,
			ImageRef:       metadata.Ref,
			ProgressWriter: pw,
		}
		if p.vtx != nil {
			p.descHandler.VertexDigest = p.vtx.Digest()
			p.descHandler.VertexName = p.vtx.Name()
		}
		cacheOpts = solver.CacheOpts(map[interface{}]interface{}{
			cache.DescHandlerKey(topDesc.Digest): p.descHandler,
		})
	}

	desc := metadata.MainManifestDesc
	if index == 0 || desc.Digest == "" {
		k, err := mainManifestKey(ctx, desc, p.Platform)
		if err != nil {
			return "", nil, false, err
		}
		return k.String(), cacheOpts, false, nil
	}
	ref, err := reference.ParseNormalizedNamed(p.Src.String())
	if err != nil {
		return "", nil, false, err
	}
	ref, err = reference.WithDigest(ref, desc.Digest)
	if err != nil {
		return "", nil, false, err
	}
	_, dt, err := imageutil.Config(ctx, ref.String(), p.Resolver, p.ContentStore, p.LeaseManager, p.Platform)
	if err != nil {
		return "", nil, false, err
	}

	k := cacheKeyFromConfig(dt).String()
	if k == "" {
		k, err := mainManifestKey(ctx, desc, p.Platform)
		if err != nil {
			return "", nil, false, err
		}
		return k.String(), cacheOpts, true, nil
	}

	return k, cacheOpts, true, nil
}

func (p *puller) Snapshot(ctx context.Context) (ir cache.ImmutableRef, err error) {
	ctx, done, err := leaseutil.WithLease(ctx, p.LeaseManager, leaseutil.MakeTemporary)
	if err != nil {
		return nil, err
	}
	defer done(ctx)
	defer p.releaseTmpLeases(ctx)

	metadata, err := p.PullMetadata(ctx)
	if err != nil {
		return nil, err
	}

	if len(metadata.Remote.Descriptors) == 0 {
		return nil, nil
	}

	var current cache.ImmutableRef
	defer func() {
		if err != nil && current != nil {
			current.Release(context.TODO())
		}
	}()

	var parent cache.ImmutableRef
	for _, layerDesc := range metadata.Remote.Descriptors {
		parent = current
		current, err = p.CacheAccessor.GetByBlob(ctx, layerDesc, parent, p.descHandler)
		if parent != nil {
			parent.Release(context.TODO())
		}
		if err != nil {
			return nil, err
		}
	}

	for _, desc := range metadata.Nonlayers {
		if err := p.LeaseManager.AddResource(ctx, leases.Lease{ID: current.ID()}, leases.Resource{
			ID:   desc.Digest.String(),
			Type: "content",
		}); err != nil {
			return nil, err
		}
	}

	if current != nil && p.Platform != nil && p.Platform.OS == "windows" && runtime.GOOS != "windows" {
		if err := markRefLayerTypeWindows(current); err != nil {
			return nil, err
		}
	}

	if p.id.RecordType != "" && cache.GetRecordType(current) == "" {
		if err := cache.SetRecordType(current, p.id.RecordType); err != nil {
			return nil, err
		}
	}

	return current, nil
}

func markRefLayerTypeWindows(ref cache.ImmutableRef) error {
	if parent := ref.Parent(); parent != nil {
		defer parent.Release(context.TODO())
		if err := markRefLayerTypeWindows(parent); err != nil {
			return err
		}
	}
	return cache.SetLayerType(ref, "windows")
}

// cacheKeyFromConfig returns a stable digest from image config. If image config
// is a known oci image we will use chainID of layers.
func cacheKeyFromConfig(dt []byte) digest.Digest {
	var img specs.Image
	err := json.Unmarshal(dt, &img)
	if err != nil {
		return digest.FromBytes(dt)
	}
	if img.RootFS.Type != "layers" || len(img.RootFS.DiffIDs) == 0 {
		return ""
	}
	return identity.ChainID(img.RootFS.DiffIDs)
}

func oneOffProgress(ctx context.Context, id string) func(err error) error {
	pw, _, _ := progress.FromContext(ctx)
	now := time.Now()
	st := progress.Status{
		Started: &now,
	}
	pw.Write(id, st)
	return func(err error) error {
		// TODO: set error on status
		now := time.Now()
		st.Completed = &now
		pw.Write(id, st)
		pw.Close()
		return err
	}
}
