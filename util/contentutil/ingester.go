package contentutil

import (
	"context"
	"time"

	"github.com/containerd/containerd/content"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
)

type IngesterFunc func(context.Context, ...content.WriterOpt) (content.Writer, error)

func (f IngesterFunc) Writer(ctx context.Context, opts ...content.WriterOpt) (content.Writer, error) {
	return f(ctx, opts...)
}

func DiscardIngester() content.Ingester {
	return IngesterFunc(func(ctx context.Context, opts ...content.WriterOpt) (content.Writer, error) {
		var wOpts content.WriterOpts
		for _, opt := range opts {
			if err := opt(&wOpts); err != nil {
				return nil, err
			}
		}
		return &discardWriter{
			startedAt:     time.Now(),
			expectedTotal: wOpts.Desc.Size,
			expectedDgst:  wOpts.Desc.Digest,
			digester:      digest.Canonical.Digester(),
		}, nil
	})
}

type discardWriter struct {
	startedAt     time.Time
	updatedAt     time.Time
	offset        int64
	expectedTotal int64
	expectedDgst  digest.Digest
	digester      digest.Digester
}

func (w *discardWriter) Write(p []byte) (n int, err error) {
	n, err = w.digester.Hash().Write(p)
	w.offset += int64(n)
	w.updatedAt = time.Now()
	return n, err
}

func (w *discardWriter) Close() error {
	return nil
}

func (w *discardWriter) Status() (content.Status, error) {
	return content.Status{
		Offset:    w.offset,
		Total:     w.expectedTotal,
		StartedAt: w.startedAt,
		UpdatedAt: w.updatedAt,
	}, nil
}

func (w *discardWriter) Digest() digest.Digest {
	return w.digester.Digest()
}

func (w *discardWriter) Commit(ctx context.Context, size int64, expected digest.Digest, _ ...content.Opt) error {
	if s := int64(w.offset); size > 0 && size != s {
		return errors.Errorf("unexpected commit size %d, expected %d", s, size)
	}
	dgst := w.Digest()
	if expected != "" && expected != dgst {
		return errors.Errorf("unexpected digest: %v != %v", dgst, expected)
	}
	if w.expectedDgst != "" && w.expectedDgst != dgst {
		return errors.Errorf("unexpected digest: %v != %v", dgst, w.expectedDgst)
	}
	return nil
}

func (w *discardWriter) Truncate(size int64) error {
	if size != 0 {
		return errors.New("Truncate: unsupported size")
	}
	w.offset = 0
	w.digester.Hash().Reset()
	return nil
}
