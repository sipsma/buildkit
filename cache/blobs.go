package cache

import (
	"context"

	"github.com/containerd/containerd/diff"
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
func (sr *immutableRef) computeBlobChain(ctx context.Context, createIfNeeded bool, compressionType compression.Type, forceCompression bool, s session.Group) error {
	if _, ok := leases.FromContext(ctx); !ok {
		return errors.Errorf("missing lease requirement for computeBlobChain")
	}

	if err := sr.finalizeLocked(ctx); err != nil {
		return err
	}

	if isTypeWindows(sr) {
		ctx = winlayers.UseWindowsLayerMode(ctx)
	}

	return computeBlobChain(ctx, sr, createIfNeeded, compressionType, forceCompression, s)
}

func computeBlobChain(ctx context.Context, sr *immutableRef, createIfNeeded bool, compressionType compression.Type, forceCompression bool, s session.Group) error {
	baseCtx := ctx
	eg, ctx := errgroup.WithContext(ctx)
	var currentDescr ocispec.Descriptor
	switch sr.parentKind() {
	case Merge:
		for _, parent := range sr.mergeParents {
			parent := parent
			eg.Go(func() error {
				return computeBlobChain(ctx, parent, createIfNeeded, compressionType, forceCompression, s)
			})
		}
	case Layer:
		eg.Go(func() error {
			return computeBlobChain(ctx, sr.layerParent, createIfNeeded, compressionType, forceCompression, s)
		})
		fallthrough
	case None:
		eg.Go(func() error {
			dp, err := g.Do(ctx, sr.ID(), func(ctx context.Context) (interface{}, error) {
				if sr.getBlob() != "" {
					if forceCompression {
						desc, err := sr.ociDesc()
						if err != nil {
							return nil, err
						}
						if err := ensureCompression(ctx, sr, desc, compressionType, s); err != nil {
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
					if sr.layerParent != nil {
						m, err := sr.layerParent.Mount(ctx, true, s)
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
					m, err := sr.Mount(ctx, true, s)
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
					descr, err = sr.cm.Differ.Compare(ctx, lower, upper,
						diff.WithMediaType(mediaType),
						diff.WithReference(sr.ID()),
					)
					if err != nil {
						return nil, err
					}
				}

				if descr.Annotations == nil {
					descr.Annotations = map[string]string{}
				}

				info, err := sr.cm.ContentStore.Info(ctx, descr.Digest)
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
					if err := ensureCompression(ctx, sr, descr, compressionType, s); err != nil {
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
		if err := sr.setBlob(baseCtx, currentDescr); err != nil {
			return err
		}
	}
	return nil
}

// setBlob associates a blob with the cache record.
// A lease must be held for the blob when calling this function
func (sr *immutableRef) setBlob(ctx context.Context, desc ocispec.Descriptor) error {
	if _, ok := leases.FromContext(ctx); !ok {
		return errors.Errorf("missing lease requirement for setBlob")
	}

	diffID, err := diffIDFromDescriptor(desc)
	if err != nil {
		return err
	}
	if _, err := sr.cm.ContentStore.Info(ctx, desc.Digest); err != nil {
		return err
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.getChainID() != "" {
		return nil
	}

	if err := sr.finalize(ctx); err != nil {
		return err
	}

	var chainIDs []digest.Digest
	var blobChainIDs []digest.Digest
	switch sr.parentKind() {
	case Merge:
		for _, p := range sr.mergeParents {
			parentChainID := p.getChainID()
			parentBlobChainID := p.getBlobChainID()
			if parentChainID == "" || parentBlobChainID == "" {
				return errors.Errorf("failed to set blob for reference with non-addressable parent")
			}
			chainIDs = append(chainIDs, parentChainID)
			blobChainIDs = append(blobChainIDs, parentBlobChainID)
		}
	case Layer:
		parentChainID := sr.layerParent.getChainID()
		parentBlobChainID := sr.layerParent.getBlobChainID()
		if parentChainID == "" || parentBlobChainID == "" {
			return errors.Errorf("failed to set blob for reference with non-addressable parent")
		}
		chainIDs = append(chainIDs, parentChainID)
		blobChainIDs = append(blobChainIDs, parentBlobChainID)
	}

	if err := sr.cm.LeaseManager.AddResource(ctx, leases.Lease{ID: sr.ID()}, leases.Resource{
		ID:   desc.Digest.String(),
		Type: "content",
	}); err != nil {
		return err
	}

	sr.queueDiffID(diffID)
	sr.queueBlob(desc.Digest)

	chainIDs = append(chainIDs, diffID)
	chainID := imagespecidentity.ChainID(chainIDs)

	blobChainIDs = append(blobChainIDs, imagespecidentity.ChainID([]digest.Digest{desc.Digest, diffID}))
	blobChainID := imagespecidentity.ChainID(blobChainIDs)

	sr.queueChainID(chainID)
	sr.queueBlobChainID(blobChainID)
	sr.queueMediaType(desc.MediaType)
	sr.queueBlobSize(desc.Size)
	if err := sr.commitMetadata(); err != nil {
		return err
	}
	return nil
}

func isTypeWindows(sr *immutableRef) bool {
	if sr.GetLayerType() == "windows" {
		return true
	}
	switch sr.parentKind() {
	case Merge:
		for _, p := range sr.mergeParents {
			if isTypeWindows(p) {
				return true
			}
		}
	case Layer:
		return isTypeWindows(sr.layerParent)
	}
	return false
}

// ensureCompression ensures the specified ref has the blob of the specified compression Type.
func ensureCompression(ctx context.Context, ref *immutableRef, desc ocispec.Descriptor, compressionType compression.Type, s session.Group) error {
	// Resolve converters
	layerConvertFunc, _, err := getConverters(desc, compressionType)
	if err != nil {
		return err
	} else if layerConvertFunc == nil {
		return nil // no need to convert
	}

	// First, lookup local content store
	if _, err := ref.getCompressionBlob(ctx, compressionType); err == nil {
		return nil // found the compression variant. no need to convert.
	}

	// Convert layer compression type
	if err := (lazyRefProvider{
		rec:     ref.cacheRecord,
		desc:    desc,
		dh:      ref.descHandlers[desc.Digest],
		session: s,
	}).Unlazy(ctx); err != nil {
		return err
	}
	newDesc, err := layerConvertFunc(ctx, ref.cm.ContentStore, desc)
	if err != nil {
		return err
	}

	// Start to track converted layer
	if err := ref.addCompressionBlob(ctx, newDesc.Digest, compressionType); err != nil {
		return err
	}
	return nil
}
