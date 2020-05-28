package cache

import (
	"context"

	"github.com/containerd/containerd/diff"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/moby/buildkit/util/compression"
	"github.com/moby/buildkit/util/flightcontrol"
	"github.com/moby/buildkit/util/winlayers"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

var g flightcontrol.Group

const containerdUncompressed = "containerd.io/uncompressed"

type CompareWithParent interface {
	CompareWithParent(ctx context.Context, ref string, opts ...diff.Opt) (ocispec.Descriptor, error)
}

var ErrNoBlobs = errors.Errorf("no blobs for snapshot")

// SetBlobChain ensures every ref in a parent chain has an associated blob in the content store. If
// a blob is missing an createBlobs is true, then the blob will be created, otherwise ErrNoBlobs will
// be returned. Caller must hold a lease when calling this function.
func (sr *immutableRef) SetBlobChain(ctx context.Context, createBlobs bool, compressionType compression.Type) error {
	if _, ok := leases.FromContext(ctx); !ok {
		return errors.Errorf("missing lease requirement for SetBlobChain")
	}

	if err := sr.Finalize(ctx, true); err != nil {
		return err
	}

	if isTypeWindows(sr) {
		ctx = winlayers.UseWindowsLayerMode(ctx)
	}

	return sr.setBlobChain(ctx, createBlobs, compressionType)
}

func (sr *immutableRef) setBlobChain(ctx context.Context, createBlobs bool, compressionType compression.Type) error {
	baseCtx := ctx
	eg, ctx := errgroup.WithContext(ctx)
	var currentDescr ocispec.Descriptor
	if sr.parent != nil {
		eg.Go(func() error {
			err := sr.parent.setBlobChain(ctx, createBlobs, compressionType)
			if err != nil {
				return err
			}
			return nil
		})
	}
	eg.Go(func() error {
		dp, err := g.Do(ctx, sr.ID(), func(ctx context.Context) (interface{}, error) {
			refInfo := sr.Info()
			if refInfo.Blob != "" {
				return nil, nil
			} else if !createBlobs {
				return nil, errors.WithStack(ErrNoBlobs)
			}

			var mediaType string
			var descr ocispec.Descriptor
			var err error

			switch compressionType {
			case compression.Uncompressed:
				mediaType = ocispec.MediaTypeImageLayer
			case compression.Gzip:
				mediaType = ocispec.MediaTypeImageLayerGzip
			default:
				return nil, errors.Errorf("unknown layer compression type")
			}

			if pc, ok := sr.cm.Differ.(CompareWithParent); ok {
				descr, err = pc.CompareWithParent(ctx, sr.ID(), diff.WithMediaType(mediaType))
				if err != nil {
					return nil, err
				}
			}
			if descr.Digest == "" {
				// reference needs to be committed
				var lower []mount.Mount
				var release func() error
				if sr.parent != nil {
					m, err := sr.parent.Mount(ctx, true)
					if err != nil {
						return nil, err
					}
					lower, release, err = m.Mount()
					if err != nil {
						return nil, err
					}
					if release != nil {
						defer release()
					}
				}
				m, err := sr.Mount(ctx, true)
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
	err := eg.Wait()
	if err != nil {
		return err
	}
	if currentDescr.Digest != "" {
		if err := sr.SetBlob(baseCtx, currentDescr); err != nil {
			return err
		}
	}
	return nil
}

func isTypeWindows(sr *immutableRef) bool {
	if GetLayerType(sr) == "windows" {
		return true
	}
	if parent := sr.parent; parent != nil {
		return isTypeWindows(parent)
	}
	return false
}
