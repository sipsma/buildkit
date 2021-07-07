package cache

import (
	"context"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/diff"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/util/compression"
	"github.com/moby/buildkit/util/flightcontrol"
	"github.com/moby/buildkit/util/winlayers"
	"github.com/opencontainers/go-digest"
	imagespecidentity "github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

var g flightcontrol.Group

const containerdUncompressed = "containerd.io/uncompressed"

var ErrNoBlobs = errors.Errorf("no blobs for snapshot")

// computeBlobChain ensures every ref in a parent chain has an associated blob in the content store. If
// a blob is missing and createIfNeeded is true, then the blob will be created, otherwise ErrNoBlobs will
// be returned. Caller must hold a lease when calling this function.
// If forceCompression is specified but the blob of compressionType doesn't exist, this function creates it.
func (sr *ImmutableRef) computeBlobChain(ctx context.Context, createIfNeeded bool, compressionType compression.Type, forceCompression bool, s session.Group) error {
	if _, ok := leases.FromContext(ctx); !ok {
		return errors.Errorf("missing lease requirement for computeBlobChain")
	}

	if err := sr.commitLocked(ctx); err != nil {
		return err
	}

	if isTypeWindows(sr) {
		ctx = winlayers.UseWindowsLayerMode(ctx)
	}

	return computeBlobChain(ctx, sr.cacheRecord, sr.descHandlers, createIfNeeded, compressionType, forceCompression, s)
}

func computeBlobChain(ctx context.Context, cr *cacheRecord, dhs DescHandlers, createIfNeeded bool, compressionType compression.Type, forceCompression bool, s session.Group) error {
	baseCtx := ctx
	eg, ctx := errgroup.WithContext(ctx)
	var currentDescr ocispec.Descriptor
	switch cr.kind() {
	case MERGE:
		for _, parent := range cr.mergeParents {
			eg.Go(func() error {
				return computeBlobChain(ctx, parent.cacheRecord, parent.descHandlers, createIfNeeded, compressionType, forceCompression, s)
			})
		}
	case LAYER:
		if cr.layerParent != nil {
			eg.Go(func() error {
				return computeBlobChain(ctx, cr.layerParent.cacheRecord, cr.layerParent.descHandlers, createIfNeeded, compressionType, forceCompression, s)
			})
		}

		eg.Go(func() error {
			dp, err := g.Do(ctx, cr.ID(), func(ctx context.Context) (interface{}, error) {
				if cr.getBlob() != "" {
					if forceCompression {
						desc, err := cr.ociDesc()
						if err != nil {
							return nil, err
						}
						if err := ensureCompression(ctx, cr, dhs, desc, compressionType, s); err != nil {
							return nil, err
						}
					}
					return nil, nil
				} else if !createIfNeeded {
					return nil, errors.WithStack(ErrNoBlobs)
				}

				var mediaType string
				switch compressionType {
				case compression.Uncompressed:
					mediaType = ocispec.MediaTypeImageLayer
				case compression.Gzip:
					mediaType = ocispec.MediaTypeImageLayerGzip
				default:
					return nil, errors.Errorf("unknown layer compression type: %q", compressionType)
				}

				var descr ocispec.Descriptor
				var err error

				if descr.Digest == "" {
					// reference needs to be committed
					var lower []mount.Mount
					if cr.layerParent != nil {
						m, err := cr.layerParent.Mount(ctx, true, s)
						if err != nil {
							return nil, err
						}
						var release func() error
						lower, release, err = m.Mount()
						if err != nil {
							return nil, err
						}
						if release != nil {
							defer release()
						}
					}
					m, err := cr.Mount(ctx, true, dhs, s)
					if err != nil {
						return nil, err
					}
					upper, release, err := m.Mount()
					if err != nil {
						return nil, err
					}
					if release != nil {
						defer release()
					}
					descr, err = cr.cm.Differ.Compare(ctx, lower, upper,
						diff.WithMediaType(mediaType),
						diff.WithReference(cr.ID()),
					)
					if err != nil {
						return nil, err
					}
				}

				if descr.Annotations == nil {
					descr.Annotations = map[string]string{}
				}

				info, err := cr.cm.ContentStore.Info(ctx, descr.Digest)
				if err != nil {
					return nil, err
				}

				if diffID, ok := info.Labels[containerdUncompressed]; ok {
					descr.Annotations[containerdUncompressed] = diffID
				} else if compressionType == compression.Uncompressed {
					descr.Annotations[containerdUncompressed] = descr.Digest.String()
				} else {
					return nil, errors.Errorf("unknown layer compression type")
				}

				if forceCompression {
					if err := ensureCompression(ctx, cr, dhs, descr, compressionType, s); err != nil {
						return nil, err
					}
				}

				return descr, nil

			})
			if err != nil {
				return err
			}

			if dp != nil {
				currentDescr = dp.(ocispec.Descriptor)
			}
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		return err
	}
	if currentDescr.Digest != "" {
		if err := cr.setBlob(baseCtx, currentDescr); err != nil {
			return err
		}
	}
	return nil
}

// setBlob associates a blob with the cache record.
// A lease must be held for the blob when calling this function
// Caller should call Info() for knowing what current values are actually set
func (cr *cacheRecord) setBlob(ctx context.Context, desc ocispec.Descriptor) error {
	if _, ok := leases.FromContext(ctx); !ok {
		return errors.Errorf("missing lease requirement for setBlob")
	}

	diffID, err := diffIDFromDescriptor(desc)
	if err != nil {
		return err
	}
	if _, err := cr.cm.ContentStore.Info(ctx, desc.Digest); err != nil {
		return err
	}

	cr.mu.Lock()
	defer cr.mu.Unlock()

	if cr.getChainID() != "" {
		return nil
	}

	if err := cr.commit(ctx); err != nil {
		return err
	}

	// TODO think through this for merge refs, especially export stuff
	p := cr.layerParent
	var parentChainID digest.Digest
	var parentBlobChainID digest.Digest
	if p != nil {
		parentChainID = p.getChainID()
		parentBlobChainID = p.getBlobChainID()
		if parentChainID == "" || parentBlobChainID == "" {
			return errors.Errorf("failed to set blob for reference with non-addressable parent")
		}
	}

	if err := cr.cm.LeaseManager.AddResource(ctx, leases.Lease{ID: cr.ID()}, leases.Resource{
		ID:   desc.Digest.String(),
		Type: "content",
	}); err != nil {
		return err
	}

	cr.queueDiffID(diffID)
	cr.queueBlob(desc.Digest)
	chainID := diffID
	blobChainID := imagespecidentity.ChainID([]digest.Digest{desc.Digest, diffID})
	if parentChainID != "" {
		chainID = imagespecidentity.ChainID([]digest.Digest{parentChainID, chainID})
		blobChainID = imagespecidentity.ChainID([]digest.Digest{parentBlobChainID, blobChainID})
	}
	cr.queueChainID(chainID)
	cr.queueBlobChainID(blobChainID)
	cr.queueMediaType(desc.MediaType)
	cr.queueBlobSize(desc.Size)
	if err := cr.commitMetadata(); err != nil {
		return err
	}
	return nil
}

func (cr *cacheRecord) ociDesc() (ocispec.Descriptor, error) {
	desc := ocispec.Descriptor{
		Digest:      cr.getBlob(),
		Size:        cr.getBlobSize(),
		MediaType:   cr.getMediaType(),
		Annotations: make(map[string]string),
	}

	diffID := cr.getDiffID()
	if diffID != "" {
		desc.Annotations["containerd.io/uncompressed"] = string(diffID)
	}

	createdAt := cr.GetCreatedAt()
	if !createdAt.IsZero() {
		createdAt, err := createdAt.MarshalText()
		if err != nil {
			return ocispec.Descriptor{}, err
		}
		desc.Annotations["buildkit/createdat"] = string(createdAt)
	}

	return desc, nil
}

const compressionVariantDigestLabelPrefix = "buildkit.io/compression/digest."

func compressionVariantDigestLabel(compressionType compression.Type) string {
	return compressionVariantDigestLabelPrefix + compressionType.String()
}

// TODO changed this from ImmutableRef method to cacheRecord, need to add any new validation it's not mutable or similar/
func (cr *cacheRecord) getCompressionBlob(ctx context.Context, compressionType compression.Type) (content.Info, error) {
	cs := cr.cm.ContentStore
	info, err := cs.Info(ctx, cr.getBlob())
	if err != nil {
		return content.Info{}, err
	}
	dgstS, ok := info.Labels[compressionVariantDigestLabel(compressionType)]
	if ok {
		dgst, err := digest.Parse(dgstS)
		if err != nil {
			return content.Info{}, err
		}
		return cs.Info(ctx, dgst)
	}
	return content.Info{}, errdefs.ErrNotFound
}

func (cr *cacheRecord) addCompressionBlob(ctx context.Context, dgst digest.Digest, compressionType compression.Type) error {
	cs := cr.cm.ContentStore
	if err := cr.cm.LeaseManager.AddResource(ctx, leases.Lease{ID: cr.ID()}, leases.Resource{
		ID:   dgst.String(),
		Type: "content",
	}); err != nil {
		return err
	}
	info, err := cs.Info(ctx, cr.getBlob())
	if err != nil {
		return err
	}
	if info.Labels == nil {
		info.Labels = make(map[string]string)
	}
	cachedVariantLabel := compressionVariantDigestLabel(compressionType)
	info.Labels[cachedVariantLabel] = dgst.String()
	if _, err := cs.Update(ctx, info, "labels."+cachedVariantLabel); err != nil {
		return err
	}
	return nil
}

func isTypeWindows(sr *ImmutableRef) bool {
	if sr.GetLayerType() == "windows" {
		return true
	}
	// TODO handle merge ref
	if parent := sr.layerParent; parent != nil {
		return isTypeWindows(parent)
	}
	return false
}

// ensureCompression ensures the specified ref has the blob of the specified compression Type.
func ensureCompression(ctx context.Context, rec *cacheRecord, dhs DescHandlers, desc ocispec.Descriptor, compressionType compression.Type, s session.Group) error {
	// Resolve converters
	layerConvertFunc, _, err := getConverters(desc, compressionType)
	if err != nil {
		return err
	} else if layerConvertFunc == nil {
		return nil // no need to convert
	}

	// First, lookup local content store
	if _, err := rec.getCompressionBlob(ctx, compressionType); err == nil {
		return nil // found the compression variant. no need to convert.
	}

	// Convert layer compression type
	if err := (lazyRefProvider{
		rec:     rec,
		desc:    desc,
		dh:      dhs[desc.Digest],
		session: s,
	}).Unlazy(ctx); err != nil {
		return err
	}
	newDesc, err := layerConvertFunc(ctx, rec.cm.ContentStore, desc)
	if err != nil {
		return err
	}

	// Start to track converted layer
	if err := rec.addCompressionBlob(ctx, newDesc.Digest, compressionType); err != nil {
		return err
	}
	return nil
}
