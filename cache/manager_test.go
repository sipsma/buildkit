package cache

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/content/local"
	"github.com/containerd/containerd/diff/apply"
	"github.com/containerd/containerd/leases"
	ctdmetadata "github.com/containerd/containerd/metadata"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/native"
	"github.com/moby/buildkit/cache/metadata"
	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/snapshot"
	containerdsnapshot "github.com/moby/buildkit/snapshot/containerd"
	"github.com/moby/buildkit/util/leaseutil"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

type cmOpt struct {
	snapshotterName string
	snapshotter     snapshots.Snapshotter
	tmpdir          string
}

type cmOut struct {
	manager Manager
	lm      leases.Manager
	cs      content.Store
}

func newCacheManager(ctx context.Context, opt cmOpt) (co *cmOut, cleanup func() error, err error) {
	ns, ok := namespaces.Namespace(ctx)
	if !ok {
		return nil, nil, errors.Errorf("namespace required for test")
	}

	if opt.snapshotterName == "" {
		opt.snapshotterName = "native"
	}

	tmpdir, err := ioutil.TempDir("", "cachemanager")
	if err != nil {
		return nil, nil, err
	}

	defers := make([]func() error, 0)
	cleanup = func() error {
		var err error
		for i := range defers {
			if err1 := defers[len(defers)-1-i](); err1 != nil && err == nil {
				err = err1
			}
		}
		return err
	}
	defer func() {
		if err != nil && cleanup != nil {
			cleanup()
		}
	}()
	if opt.tmpdir == "" {
		defers = append(defers, func() error {
			return os.RemoveAll(tmpdir)
		})
	} else {
		os.RemoveAll(tmpdir)
		tmpdir = opt.tmpdir
	}

	if opt.snapshotter == nil {
		snapshotter, err := native.NewSnapshotter(filepath.Join(tmpdir, "snapshots"))
		if err != nil {
			return nil, nil, err
		}
		opt.snapshotter = snapshotter
	}

	store, err := local.NewStore(tmpdir)
	if err != nil {
		return nil, nil, err
	}

	db, err := bolt.Open(filepath.Join(tmpdir, "containerdmeta.db"), 0644, nil)
	if err != nil {
		return nil, nil, err
	}
	defers = append(defers, func() error {
		return db.Close()
	})

	mdb := ctdmetadata.NewDB(db, store, map[string]snapshots.Snapshotter{
		opt.snapshotterName: opt.snapshotter,
	})
	if err := mdb.Init(context.TODO()); err != nil {
		return nil, nil, err
	}

	lm := ctdmetadata.NewLeaseManager(mdb)

	cm, err := NewManager(ManagerOpt{
		Snapshotter:       snapshot.FromContainerdSnapshotter(opt.snapshotterName, containerdsnapshot.NSSnapshotter(ns, mdb.Snapshotter(opt.snapshotterName)), nil),
		ContentStore:      mdb.ContentStore(),
		LeaseManager:      leaseutil.WithNamespace(lm, ns),
		GarbageCollect:    mdb.GarbageCollect,
		Applier:           apply.NewFileSystemApplier(mdb.ContentStore()),
		MetadataStoreRoot: tmpdir,
	})
	if err != nil {
		return nil, nil, err
	}
	return &cmOut{
		manager: cm,
		lm:      lm,
		cs:      mdb.ContentStore(),
	}, cleanup, nil
}

func TestManager(t *testing.T) {
	t.Parallel()

	ctx := namespaces.WithNamespace(context.Background(), "buildkit-test")

	tmpdir, err := ioutil.TempDir("", "cachemanager")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	snapshotter, err := native.NewSnapshotter(filepath.Join(tmpdir, "snapshots"))
	require.NoError(t, err)

	co, cleanup, err := newCacheManager(ctx, cmOpt{
		snapshotter:     snapshotter,
		snapshotterName: "native",
	})
	require.NoError(t, err)

	defer cleanup()
	cm := co.manager

	_, err = cm.Get(ctx, "foobar")
	require.Error(t, err)

	checkDiskUsage(ctx, t, cm, 0, 0)

	active, err := cm.New(ctx, nil, nil, CachePolicyRetain)
	require.NoError(t, err)

	m, err := active.Mount(ctx, false, nil)
	require.NoError(t, err)

	lm := snapshot.LocalMounter(m)
	target, err := lm.Mount()
	require.NoError(t, err)

	fi, err := os.Stat(target)
	require.NoError(t, err)
	require.Equal(t, fi.IsDir(), true)

	err = lm.Unmount()
	require.NoError(t, err)

	_, err = cm.GetMutable(ctx, active.ID())
	require.Error(t, err)
	require.Equal(t, true, errors.Is(err, ErrLocked))

	checkDiskUsage(ctx, t, cm, 1, 0)

	snap, err := active.Commit(ctx)
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 1, 0)

	_, err = cm.GetMutable(ctx, active.ID())
	require.Error(t, err)
	require.Equal(t, true, errors.Is(err, ErrLocked))

	err = snap.Release(ctx)
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 0, 1)

	active, err = cm.GetMutable(ctx, active.ID())
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 1, 0)

	snap, err = active.Commit(ctx)
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 1, 0)

	err = snap.(*immutableRef).finalizeLocked(ctx)
	require.NoError(t, err)

	err = snap.Release(ctx)
	require.NoError(t, err)

	_, err = cm.GetMutable(ctx, active.ID())
	require.Error(t, err)
	require.Equal(t, true, errors.Is(err, errInvalid))

	_, err = cm.GetMutable(ctx, snap.ID())
	require.Error(t, err)
	require.Equal(t, true, errors.Is(err, errInvalid))

	snap, err = cm.Get(ctx, snap.ID())
	require.NoError(t, err)

	snap2, err := cm.Get(ctx, snap.ID())
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 1, 0)

	err = snap.Release(ctx)
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 1, 0)

	active2, err := cm.New(ctx, snap2, nil, CachePolicyRetain)
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 2, 0)

	snap3, err := active2.Commit(ctx)
	require.NoError(t, err)

	err = snap2.Release(ctx)
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 2, 0)

	err = snap3.Release(ctx)
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 0, 2)

	buf := pruneResultBuffer()
	err = cm.Prune(ctx, buf.C, client.PruneInfo{})
	buf.close()
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 0, 0)

	require.Equal(t, len(buf.all), 2)

	err = cm.Close()
	require.NoError(t, err)

	dirs, err := ioutil.ReadDir(filepath.Join(tmpdir, "snapshots/snapshots"))
	require.NoError(t, err)
	require.Equal(t, 0, len(dirs))
}

func TestLazyGetByBlob(t *testing.T) {
	t.Parallel()
	ctx := namespaces.WithNamespace(context.Background(), "buildkit-test")

	tmpdir, err := ioutil.TempDir("", "cachemanager")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	snapshotter, err := native.NewSnapshotter(filepath.Join(tmpdir, "snapshots"))
	require.NoError(t, err)

	co, cleanup, err := newCacheManager(ctx, cmOpt{
		snapshotter:     snapshotter,
		snapshotterName: "native",
	})
	require.NoError(t, err)
	defer cleanup()
	cm := co.manager

	// Test for #2226 https://github.com/moby/buildkit/issues/2226, create lazy blobs with the same diff ID but
	// different digests (due to different compression) and make sure GetByBlob still works
	_, desc, err := mapToBlob(map[string]string{"foo": "bar"}, true)
	require.NoError(t, err)
	descHandlers := DescHandlers(make(map[digest.Digest]*DescHandler))
	descHandlers[desc.Digest] = &DescHandler{}
	diffID, err := diffIDFromDescriptor(desc)
	require.NoError(t, err)

	_, err = cm.GetByBlob(ctx, desc, nil, descHandlers)
	require.NoError(t, err)

	_, desc2, err := mapToBlob(map[string]string{"foo": "bar"}, false)
	require.NoError(t, err)
	descHandlers2 := DescHandlers(make(map[digest.Digest]*DescHandler))
	descHandlers2[desc2.Digest] = &DescHandler{}
	diffID2, err := diffIDFromDescriptor(desc2)
	require.NoError(t, err)

	require.NotEqual(t, desc.Digest, desc2.Digest)
	require.Equal(t, diffID, diffID2)

	_, err = cm.GetByBlob(ctx, desc2, nil, descHandlers2)
	require.NoError(t, err)
}

func TestSnapshotExtract(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Depends on unimplemented containerd bind-mount support on Windows")
	}

	t.Parallel()
	ctx := namespaces.WithNamespace(context.Background(), "buildkit-test")

	tmpdir, err := ioutil.TempDir("", "cachemanager")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	snapshotter, err := native.NewSnapshotter(filepath.Join(tmpdir, "snapshots"))
	require.NoError(t, err)

	co, cleanup, err := newCacheManager(ctx, cmOpt{
		snapshotter:     snapshotter,
		snapshotterName: "native",
	})
	require.NoError(t, err)

	defer cleanup()

	cm := co.manager

	b, desc, err := mapToBlob(map[string]string{"foo": "bar"}, true)
	require.NoError(t, err)

	err = content.WriteBlob(ctx, co.cs, "ref1", bytes.NewBuffer(b), desc)
	require.NoError(t, err)

	snap, err := cm.GetByBlob(ctx, desc, nil)
	require.NoError(t, err)

	require.Equal(t, false, !snap.(*immutableRef).getBlobOnly())

	b2, desc2, err := mapToBlob(map[string]string{"foo": "bar123"}, true)
	require.NoError(t, err)

	err = content.WriteBlob(ctx, co.cs, "ref1", bytes.NewBuffer(b2), desc2)
	require.NoError(t, err)

	snap2, err := cm.GetByBlob(ctx, desc2, snap)
	require.NoError(t, err)

	size, err := snap2.(*immutableRef).size(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(len(b2)), size)

	require.Equal(t, false, !snap2.(*immutableRef).getBlobOnly())

	dirs, err := ioutil.ReadDir(filepath.Join(tmpdir, "snapshots/snapshots"))
	require.NoError(t, err)
	require.Equal(t, 0, len(dirs))

	checkNumBlobs(ctx, t, co.cs, 2)

	err = snap2.Extract(ctx, nil)
	require.NoError(t, err)

	require.Equal(t, true, !snap.(*immutableRef).getBlobOnly())
	require.Equal(t, true, !snap2.(*immutableRef).getBlobOnly())

	dirs, err = ioutil.ReadDir(filepath.Join(tmpdir, "snapshots/snapshots"))
	require.NoError(t, err)
	require.Equal(t, 2, len(dirs))

	buf := pruneResultBuffer()
	err = cm.Prune(ctx, buf.C, client.PruneInfo{})
	buf.close()
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 2, 0)

	require.Equal(t, len(buf.all), 0)

	dirs, err = ioutil.ReadDir(filepath.Join(tmpdir, "snapshots/snapshots"))
	require.NoError(t, err)
	require.Equal(t, 2, len(dirs))

	checkNumBlobs(ctx, t, co.cs, 2)

	id := snap.ID()

	err = snap.Release(context.TODO())
	require.NoError(t, err)

	buf = pruneResultBuffer()
	err = cm.Prune(ctx, buf.C, client.PruneInfo{})
	buf.close()
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 2, 0)

	dirs, err = ioutil.ReadDir(filepath.Join(tmpdir, "snapshots/snapshots"))
	require.NoError(t, err)
	require.Equal(t, 2, len(dirs))

	snap, err = cm.Get(ctx, id)
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 2, 0)

	err = snap2.Release(context.TODO())
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 1, 1)

	buf = pruneResultBuffer()
	err = cm.Prune(ctx, buf.C, client.PruneInfo{})
	buf.close()
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 1, 0)

	require.Equal(t, len(buf.all), 1)

	dirs, err = ioutil.ReadDir(filepath.Join(tmpdir, "snapshots/snapshots"))
	require.NoError(t, err)
	require.Equal(t, 1, len(dirs))

	checkNumBlobs(ctx, t, co.cs, 1)

	err = snap.Release(context.TODO())
	require.NoError(t, err)

	buf = pruneResultBuffer()
	err = cm.Prune(ctx, buf.C, client.PruneInfo{})
	buf.close()
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 0, 0)

	dirs, err = ioutil.ReadDir(filepath.Join(tmpdir, "snapshots/snapshots"))
	require.NoError(t, err)
	require.Equal(t, 0, len(dirs))

	checkNumBlobs(ctx, t, co.cs, 0)
}

func TestExtractOnMutable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Depends on unimplemented containerd bind-mount support on Windows")
	}

	t.Parallel()
	ctx := namespaces.WithNamespace(context.Background(), "buildkit-test")

	tmpdir, err := ioutil.TempDir("", "cachemanager")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	snapshotter, err := native.NewSnapshotter(filepath.Join(tmpdir, "snapshots"))
	require.NoError(t, err)

	co, cleanup, err := newCacheManager(ctx, cmOpt{
		snapshotter:     snapshotter,
		snapshotterName: "native",
	})
	require.NoError(t, err)

	defer cleanup()

	cm := co.manager

	active, err := cm.New(ctx, nil, nil)
	require.NoError(t, err)

	snap, err := active.Commit(ctx)
	require.NoError(t, err)

	b, desc, err := mapToBlob(map[string]string{"foo": "bar"}, true)
	require.NoError(t, err)

	err = content.WriteBlob(ctx, co.cs, "ref1", bytes.NewBuffer(b), desc)
	require.NoError(t, err)

	b2, desc2, err := mapToBlob(map[string]string{"foo2": "1"}, true)
	require.NoError(t, err)

	err = content.WriteBlob(ctx, co.cs, "ref2", bytes.NewBuffer(b2), desc2)
	require.NoError(t, err)

	_, err = cm.GetByBlob(ctx, desc2, snap)
	require.Error(t, err)

	leaseCtx, done, err := leaseutil.WithLease(ctx, co.lm, leases.WithExpiration(0))
	require.NoError(t, err)

	err = snap.(*immutableRef).setBlob(leaseCtx, desc)
	done(context.TODO())
	require.NoError(t, err)

	snap2, err := cm.GetByBlob(ctx, desc2, snap)
	require.NoError(t, err)

	err = snap.Release(context.TODO())
	require.NoError(t, err)

	require.Equal(t, false, !snap2.(*immutableRef).getBlobOnly())

	size, err := snap2.(*immutableRef).size(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(len(b2)), size)

	dirs, err := ioutil.ReadDir(filepath.Join(tmpdir, "snapshots/snapshots"))
	require.NoError(t, err)
	require.Equal(t, 1, len(dirs))

	checkNumBlobs(ctx, t, co.cs, 2)

	err = snap2.Extract(ctx, nil)
	require.NoError(t, err)

	require.Equal(t, true, !snap.(*immutableRef).getBlobOnly())
	require.Equal(t, true, !snap2.(*immutableRef).getBlobOnly())

	buf := pruneResultBuffer()
	err = cm.Prune(ctx, buf.C, client.PruneInfo{})
	buf.close()
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 2, 0)

	require.Equal(t, len(buf.all), 0)

	dirs, err = ioutil.ReadDir(filepath.Join(tmpdir, "snapshots/snapshots"))
	require.NoError(t, err)
	require.Equal(t, 2, len(dirs))

	err = snap2.Release(context.TODO())
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 0, 2)

	buf = pruneResultBuffer()
	err = cm.Prune(ctx, buf.C, client.PruneInfo{})
	buf.close()
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 0, 0)

	require.Equal(t, len(buf.all), 2)

	dirs, err = ioutil.ReadDir(filepath.Join(tmpdir, "snapshots/snapshots"))
	require.NoError(t, err)
	require.Equal(t, 0, len(dirs))

	checkNumBlobs(ctx, t, co.cs, 0)
}

func TestSetBlob(t *testing.T) {
	t.Parallel()
	ctx := namespaces.WithNamespace(context.Background(), "buildkit-test")

	tmpdir, err := ioutil.TempDir("", "cachemanager")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	snapshotter, err := native.NewSnapshotter(filepath.Join(tmpdir, "snapshots"))
	require.NoError(t, err)

	co, cleanup, err := newCacheManager(ctx, cmOpt{
		snapshotter:     snapshotter,
		snapshotterName: "native",
	})
	require.NoError(t, err)

	defer cleanup()

	ctx, done, err := leaseutil.WithLease(ctx, co.lm, leaseutil.MakeTemporary)
	require.NoError(t, err)
	defer done(context.TODO())

	cm := co.manager

	active, err := cm.New(ctx, nil, nil)
	require.NoError(t, err)

	snap, err := active.Commit(ctx)
	require.NoError(t, err)

	snapRef := snap.(*immutableRef)
	require.Equal(t, "", string(snapRef.getDiffID()))
	require.Equal(t, "", string(snapRef.getBlob()))
	require.Equal(t, "", string(snapRef.getChainID()))
	require.Equal(t, "", string(snapRef.getBlobChainID()))
	require.Equal(t, !snapRef.getBlobOnly(), true)

	ctx, clean, err := leaseutil.WithLease(ctx, co.lm)
	require.NoError(t, err)
	defer clean(context.TODO())

	b, desc, err := mapToBlob(map[string]string{"foo": "bar"}, true)
	require.NoError(t, err)

	err = content.WriteBlob(ctx, co.cs, "ref1", bytes.NewBuffer(b), desc)
	require.NoError(t, err)

	err = snap.(*immutableRef).setBlob(ctx, ocispec.Descriptor{
		Digest: digest.FromBytes([]byte("foobar")),
		Annotations: map[string]string{
			"containerd.io/uncompressed": digest.FromBytes([]byte("foobar2")).String(),
		},
	})
	require.Error(t, err)

	err = snap.(*immutableRef).setBlob(ctx, desc)
	require.NoError(t, err)

	snapRef = snap.(*immutableRef)
	require.Equal(t, desc.Annotations["containerd.io/uncompressed"], string(snapRef.getDiffID()))
	require.Equal(t, desc.Digest, snapRef.getBlob())
	require.Equal(t, desc.MediaType, snapRef.getMediaType())
	require.Equal(t, snapRef.getDiffID(), snapRef.getChainID())
	require.Equal(t, digest.FromBytes([]byte(desc.Digest+" "+snapRef.getDiffID())), snapRef.getBlobChainID())
	require.Equal(t, snap.ID(), snapRef.getSnapshotID())
	require.Equal(t, !snapRef.getBlobOnly(), true)

	active, err = cm.New(ctx, snap, nil)
	require.NoError(t, err)

	snap2, err := active.Commit(ctx)
	require.NoError(t, err)

	b2, desc2, err := mapToBlob(map[string]string{"foo2": "bar2"}, true)
	require.NoError(t, err)

	err = content.WriteBlob(ctx, co.cs, "ref2", bytes.NewBuffer(b2), desc2)
	require.NoError(t, err)

	err = snap2.(*immutableRef).setBlob(ctx, desc2)
	require.NoError(t, err)

	snapRef2 := snap2.(*immutableRef)
	require.Equal(t, desc2.Annotations["containerd.io/uncompressed"], string(snapRef2.getDiffID()))
	require.Equal(t, desc2.Digest, snapRef2.getBlob())
	require.Equal(t, desc2.MediaType, snapRef2.getMediaType())
	require.Equal(t, digest.FromBytes([]byte(snapRef.getChainID()+" "+snapRef2.getDiffID())), snapRef2.getChainID())
	require.Equal(t, digest.FromBytes([]byte(snapRef.getBlobChainID()+" "+digest.FromBytes([]byte(desc2.Digest+" "+snapRef2.getDiffID())))), snapRef2.getBlobChainID())
	require.Equal(t, snap2.ID(), snapRef2.getSnapshotID())
	require.Equal(t, !snapRef2.getBlobOnly(), true)

	b3, desc3, err := mapToBlob(map[string]string{"foo3": "bar3"}, true)
	require.NoError(t, err)

	err = content.WriteBlob(ctx, co.cs, "ref3", bytes.NewBuffer(b3), desc3)
	require.NoError(t, err)

	snap3, err := cm.GetByBlob(ctx, desc3, snap)
	require.NoError(t, err)

	snapRef3 := snap3.(*immutableRef)
	require.Equal(t, desc3.Annotations["containerd.io/uncompressed"], string(snapRef3.getDiffID()))
	require.Equal(t, desc3.Digest, snapRef3.getBlob())
	require.Equal(t, desc3.MediaType, snapRef3.getMediaType())
	require.Equal(t, digest.FromBytes([]byte(snapRef.getChainID()+" "+snapRef3.getDiffID())), snapRef3.getChainID())
	require.Equal(t, digest.FromBytes([]byte(snapRef.getBlobChainID()+" "+digest.FromBytes([]byte(desc3.Digest+" "+snapRef3.getDiffID())))), snapRef3.getBlobChainID())
	require.Equal(t, string(snapRef3.getChainID()), snapRef3.getSnapshotID())
	require.Equal(t, !snapRef3.getBlobOnly(), false)

	// snap4 is same as snap2
	snap4, err := cm.GetByBlob(ctx, desc2, snap)
	require.NoError(t, err)

	require.Equal(t, snap2.ID(), snap4.ID())

	// snap5 is same different blob but same diffID as snap2
	b5, desc5, err := mapToBlob(map[string]string{"foo5": "bar5"}, true)
	require.NoError(t, err)

	desc5.Annotations["containerd.io/uncompressed"] = snapRef2.getDiffID().String()

	err = content.WriteBlob(ctx, co.cs, "ref5", bytes.NewBuffer(b5), desc5)
	require.NoError(t, err)

	snap5, err := cm.GetByBlob(ctx, desc5, snap)
	require.NoError(t, err)

	snapRef5 := snap5.(*immutableRef)
	require.NotEqual(t, snap2.ID(), snap5.ID())
	require.Equal(t, snapRef2.getSnapshotID(), snapRef5.getSnapshotID())
	require.Equal(t, snapRef2.getDiffID(), snapRef5.getDiffID())
	require.Equal(t, desc5.Digest, snapRef5.getBlob())

	require.Equal(t, snapRef2.getChainID(), snapRef5.getChainID())
	require.NotEqual(t, snapRef2.getBlobChainID(), snapRef5.getBlobChainID())
	require.Equal(t, digest.FromBytes([]byte(snapRef.getBlobChainID()+" "+digest.FromBytes([]byte(desc5.Digest+" "+snapRef2.getDiffID())))), snapRef5.getBlobChainID())

	// snap6 is a child of snap3
	b6, desc6, err := mapToBlob(map[string]string{"foo6": "bar6"}, true)
	require.NoError(t, err)

	err = content.WriteBlob(ctx, co.cs, "ref6", bytes.NewBuffer(b6), desc6)
	require.NoError(t, err)

	snap6, err := cm.GetByBlob(ctx, desc6, snap3)
	require.NoError(t, err)

	snapRef6 := snap6.(*immutableRef)
	require.Equal(t, desc6.Annotations["containerd.io/uncompressed"], string(snapRef6.getDiffID()))
	require.Equal(t, desc6.Digest, snapRef6.getBlob())
	require.Equal(t, digest.FromBytes([]byte(snapRef3.getChainID()+" "+snapRef6.getDiffID())), snapRef6.getChainID())
	require.Equal(t, digest.FromBytes([]byte(snapRef3.getBlobChainID()+" "+digest.FromBytes([]byte(snapRef6.getBlob()+" "+snapRef6.getDiffID())))), snapRef6.getBlobChainID())
	require.Equal(t, string(snapRef6.getChainID()), snapRef6.getSnapshotID())
	require.Equal(t, !snapRef6.getBlobOnly(), false)

	_, err = cm.GetByBlob(ctx, ocispec.Descriptor{
		Digest: digest.FromBytes([]byte("notexist")),
		Annotations: map[string]string{
			"containerd.io/uncompressed": digest.FromBytes([]byte("notexist")).String(),
		},
	}, snap3)
	require.Error(t, err)

	clean(context.TODO())

	//snap.SetBlob()
}

func TestPrune(t *testing.T) {
	t.Parallel()
	ctx := namespaces.WithNamespace(context.Background(), "buildkit-test")

	tmpdir, err := ioutil.TempDir("", "cachemanager")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	snapshotter, err := native.NewSnapshotter(filepath.Join(tmpdir, "snapshots"))
	require.NoError(t, err)

	co, cleanup, err := newCacheManager(ctx, cmOpt{
		snapshotter:     snapshotter,
		snapshotterName: "native",
	})
	require.NoError(t, err)

	defer cleanup()
	cm := co.manager

	active, err := cm.New(ctx, nil, nil)
	require.NoError(t, err)

	snap, err := active.Commit(ctx)
	require.NoError(t, err)

	active, err = cm.New(ctx, snap, nil, CachePolicyRetain)
	require.NoError(t, err)

	snap2, err := active.Commit(ctx)
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 2, 0)

	dirs, err := ioutil.ReadDir(filepath.Join(tmpdir, "snapshots/snapshots"))
	require.NoError(t, err)
	require.Equal(t, 2, len(dirs))

	// prune with keeping refs does nothing
	buf := pruneResultBuffer()
	err = cm.Prune(ctx, buf.C, client.PruneInfo{})
	buf.close()
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 2, 0)
	require.Equal(t, len(buf.all), 0)

	dirs, err = ioutil.ReadDir(filepath.Join(tmpdir, "snapshots/snapshots"))
	require.NoError(t, err)
	require.Equal(t, 2, len(dirs))

	err = snap2.Release(ctx)
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 1, 1)

	// prune with keeping single refs deletes one
	buf = pruneResultBuffer()
	err = cm.Prune(ctx, buf.C, client.PruneInfo{})
	buf.close()
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 1, 0)
	require.Equal(t, len(buf.all), 1)

	dirs, err = ioutil.ReadDir(filepath.Join(tmpdir, "snapshots/snapshots"))
	require.NoError(t, err)
	require.Equal(t, 1, len(dirs))

	err = snap.Release(ctx)
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 0, 1)

	active, err = cm.New(ctx, snap, nil, CachePolicyRetain)
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 2, 0)

	snap2, err = active.Commit(ctx)
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 2, 0)

	err = snap.Release(ctx)
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 2, 0)

	// prune with parent released does nothing
	buf = pruneResultBuffer()
	err = cm.Prune(ctx, buf.C, client.PruneInfo{})
	buf.close()
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 2, 0)
	require.Equal(t, len(buf.all), 0)

	// releasing last reference
	err = snap2.Release(ctx)
	require.NoError(t, err)
	checkDiskUsage(ctx, t, cm, 0, 2)

	buf = pruneResultBuffer()
	err = cm.Prune(ctx, buf.C, client.PruneInfo{})
	buf.close()
	require.NoError(t, err)

	checkDiskUsage(ctx, t, cm, 0, 0)
	require.Equal(t, len(buf.all), 2)

	dirs, err = ioutil.ReadDir(filepath.Join(tmpdir, "snapshots/snapshots"))
	require.NoError(t, err)
	require.Equal(t, 0, len(dirs))
}

func TestLazyCommit(t *testing.T) {
	t.Parallel()

	ctx := namespaces.WithNamespace(context.Background(), "buildkit-test")

	tmpdir, err := ioutil.TempDir("", "cachemanager")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	snapshotter, err := native.NewSnapshotter(filepath.Join(tmpdir, "snapshots"))
	require.NoError(t, err)

	co, cleanup, err := newCacheManager(ctx, cmOpt{
		tmpdir:          tmpdir,
		snapshotter:     snapshotter,
		snapshotterName: "native",
	})
	require.NoError(t, err)
	cm := co.manager

	active, err := cm.New(ctx, nil, nil, CachePolicyRetain)
	require.NoError(t, err)
	activeID := active.ID()

	err = active.Release(ctx)
	require.NoError(t, err)

	active, err = cm.GetMutable(ctx, activeID)
	require.NoError(t, err)
	require.Equal(t, active.ID(), activeID)

	// after commit mutable is locked
	snap, err := active.Commit(ctx)
	require.NoError(t, err)

	_, err = cm.GetMutable(ctx, active.ID())
	require.Error(t, err)
	require.Equal(t, true, errors.Is(err, ErrLocked))

	// immutable refs still work
	snap2, err := cm.Get(ctx, snap.ID())
	require.NoError(t, err)
	require.Equal(t, snap.ID(), snap2.ID())

	err = snap.Release(ctx)
	require.NoError(t, err)

	err = snap2.Release(ctx)
	require.NoError(t, err)

	// immutable work after final release as well
	snap, err = cm.Get(ctx, snap.ID())
	require.NoError(t, err)
	require.Equal(t, snap.ID(), snap2.ID())

	// active can't be get while immutable is held
	_, err = cm.GetMutable(ctx, active.ID())
	require.Error(t, err)
	require.Equal(t, true, errors.Is(err, ErrLocked))

	err = snap.Release(ctx)
	require.NoError(t, err)

	// after release mutable becomes available again
	active2, err := cm.GetMutable(ctx, active.ID())
	require.NoError(t, err)
	// ID is different now because the previous active was committed
	require.NotEqual(t, active2.ID(), active.ID())

	_, err = cm.Get(ctx, snap.ID())
	require.Error(t, err)
	require.Equal(t, true, errors.Is(err, errNotFound))

	snap, err = active2.Commit(ctx)
	require.NoError(t, err)

	// this time finalize commit
	err = snap.(*immutableRef).finalizeLocked(ctx)
	require.NoError(t, err)

	err = snap.Release(ctx)
	require.NoError(t, err)

	// mutable is not accessible after finalize
	_, err = cm.GetMutable(ctx, active2.ID())
	require.Error(t, err)
	require.Equal(t, true, errors.Is(err, errInvalid))

	// immutable still works
	snap2, err = cm.Get(ctx, snap.ID())
	require.NoError(t, err)
	require.Equal(t, snap.ID(), snap2.ID())

	err = snap2.Release(ctx)
	require.NoError(t, err)

	// test restarting after commit
	active, err = cm.New(ctx, nil, nil, CachePolicyRetain)
	require.NoError(t, err)

	// after commit mutable is locked
	snap, err = active.Commit(ctx)
	require.NoError(t, err)

	err = cm.Close()
	require.NoError(t, err)

	cleanup()

	// we can't close snapshotter and open it twice (especially, its internal bbolt store)
	co, cleanup, err = newCacheManager(ctx, cmOpt{
		tmpdir:          tmpdir,
		snapshotter:     snapshotter,
		snapshotterName: "native",
	})
	require.NoError(t, err)
	cm = co.manager

	snap2, err = cm.Get(ctx, snap.ID())
	require.NoError(t, err)

	err = snap2.Release(ctx)
	require.NoError(t, err)

	active, err = cm.GetMutable(ctx, active.ID())
	require.NoError(t, err)

	_, err = cm.Get(ctx, snap.ID())
	require.Error(t, err)
	require.Equal(t, true, errors.Is(err, errNotFound))

	snap, err = active.Commit(ctx)
	require.NoError(t, err)

	err = cm.Close()
	require.NoError(t, err)

	cleanup()

	co, cleanup, err = newCacheManager(ctx, cmOpt{
		tmpdir:          tmpdir,
		snapshotter:     snapshotter,
		snapshotterName: "native",
	})
	require.NoError(t, err)
	cm = co.manager

	snap2, err = cm.Get(ctx, snap.ID())
	require.NoError(t, err)
	snap2ID := snap2.ID()

	err = snap2.(*immutableRef).finalizeLocked(ctx)
	require.NoError(t, err)
	require.NoError(t, snap2.SetDescription("foo"))

	err = snap2.Release(ctx)
	require.NoError(t, err)

	_, err = cm.GetMutable(ctx, active.ID())
	require.Error(t, err)
	require.Equal(t, true, errors.Is(err, errInvalid))

	active, err = cm.New(ctx, nil, nil, CachePolicyRetain)
	require.NoError(t, err)
	snap3, err := active.Commit(ctx)
	require.NoError(t, err)
	snap3ID := snap3.ID()

	// simulate the old equalMutable format to test that the migration logic in cacheManager.init works as expected
	snap3.(*immutableRef).queueEqualMutable(snap2ID)
	require.NoError(t, snap3.(*immutableRef).commitMetadata())

	require.NoError(t, snap3.Release(ctx))
	checkDiskUsage(ctx, t, cm, 0, 3)

	err = cm.Close()
	require.NoError(t, err)
	cleanup()

	co, cleanup, err = newCacheManager(ctx, cmOpt{
		tmpdir:          tmpdir,
		snapshotter:     snapshotter,
		snapshotterName: "native",
	})
	require.NoError(t, err)
	defer cleanup()
	cm = co.manager

	// there's 1 less ref now because cacheManager.init should have gotten rid of the one marked as an equalMutable
	checkDiskUsage(ctx, t, cm, 0, 2)

	snap3, err = cm.Get(ctx, snap3ID)
	require.NoError(t, err)

	_, err = cm.Get(ctx, snap2ID)
	require.True(t, errors.Is(err, errNotFound))

	checkDiskUsage(ctx, t, cm, 1, 1)

	require.Equal(t, "", snap3.(*immutableRef).getEqualMutable())
	require.Equal(t, "foo", snap3.GetDescription())
	require.Equal(t, snap2ID, snap3.(*immutableRef).getSnapshotID())
}

func checkDiskUsage(ctx context.Context, t *testing.T, cm Manager, inuse, unused int) {
	du, err := cm.DiskUsage(ctx, client.DiskUsageInfo{})
	require.NoError(t, err)
	var inuseActual, unusedActual int
	for _, r := range du {
		if r.InUse {
			inuseActual++
		} else {
			unusedActual++
		}
	}
	require.Equal(t, inuse, inuseActual)
	require.Equal(t, unused, unusedActual)
}

func checkNumBlobs(ctx context.Context, t *testing.T, cs content.Store, expected int) {
	c := 0
	err := cs.Walk(ctx, func(_ content.Info) error {
		c++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, expected, c)
}

func pruneResultBuffer() *buf {
	b := &buf{C: make(chan client.UsageInfo), closed: make(chan struct{})}
	go func() {
		for c := range b.C {
			b.all = append(b.all, c)
		}
		close(b.closed)
	}()
	return b
}

type buf struct {
	C      chan client.UsageInfo
	closed chan struct{}
	all    []client.UsageInfo
}

func (b *buf) close() {
	close(b.C)
	<-b.closed
}

type bufferCloser struct {
	*bytes.Buffer
}

func (b bufferCloser) Close() error {
	return nil
}

func mapToBlob(m map[string]string, compress bool) ([]byte, ocispec.Descriptor, error) {
	buf := bytes.NewBuffer(nil)
	sha := digest.SHA256.Digester()

	var dest io.WriteCloser = bufferCloser{buf}
	if compress {
		dest = gzip.NewWriter(buf)
	}
	tw := tar.NewWriter(io.MultiWriter(sha.Hash(), dest))

	for k, v := range m {
		if err := tw.WriteHeader(&tar.Header{
			Name: k,
			Size: int64(len(v)),
		}); err != nil {
			return nil, ocispec.Descriptor{}, err
		}
		if _, err := tw.Write([]byte(v)); err != nil {
			return nil, ocispec.Descriptor{}, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, ocispec.Descriptor{}, err
	}
	if err := dest.Close(); err != nil {
		return nil, ocispec.Descriptor{}, err
	}

	mediaType := ocispec.MediaTypeImageLayer
	if compress {
		mediaType = ocispec.MediaTypeImageLayerGzip
	}
	return buf.Bytes(), ocispec.Descriptor{
		Digest:    digest.FromBytes(buf.Bytes()),
		MediaType: mediaType,
		Size:      int64(buf.Len()),
		Annotations: map[string]string{
			"containerd.io/uncompressed": sha.Digest().String(),
		},
	}, nil
}

func (md *cacheMetadata) queueEqualMutable(s string) error {
	v, err := metadata.NewValue(s)
	if err != nil {
		return errors.Wrapf(err, "failed to create %s meta value", deprecatedKeyEqualMutable)
	}
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, deprecatedKeyEqualMutable, v)
	})
	return nil
}
