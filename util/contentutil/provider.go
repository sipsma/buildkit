package contentutil

import (
	"context"
	"io"

	"github.com/containerd/containerd/content"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

type ProviderFunc func(context.Context, ocispec.Descriptor) (content.ReaderAt, error)

func (f ProviderFunc) ReaderAt(ctx context.Context, desc ocispec.Descriptor) (content.ReaderAt, error) {
	return f(ctx, desc)
}

func ReadBlob(ctx context.Context, provider content.Provider, desc ocispec.Descriptor) ([]byte, error) {
	dt, err := content.ReadBlob(ctx, provider, desc)
	if err != nil {
		// NOTE: even if err == EOF, we might have got expected dt here.
		// For instance, http.Response.Body is known to return non-zero bytes with EOF.
		if err == io.EOF {
			if dtDigest := desc.Digest.Algorithm().FromBytes(dt); dtDigest != desc.Digest {
				err = errors.Wrapf(err, "got EOF, expected %s (%d bytes), got %s (%d bytes)",
					desc.Digest, desc.Size, dtDigest, len(dt))
			} else {
				err = nil
			}
		}
	}
	return dt, errors.WithStack(err)
}
