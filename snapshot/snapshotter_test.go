package snapshot

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/containerd/content/local"
	"github.com/containerd/containerd/diff/apply"
	"github.com/containerd/containerd/diff/walking"
	"github.com/containerd/containerd/leases"
	ctdmetadata "github.com/containerd/containerd/metadata"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/native"
	"github.com/containerd/containerd/snapshots/overlay"
	"github.com/hashicorp/go-multierror"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/util/leaseutil"
	"github.com/moby/buildkit/util/winlayers"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

func NewSnapshotter(ctx context.Context, snapshotterName string) (_ context.Context, _ *fromContainerd, _ func() error, rerr error) {
	ns := "buildkit-test"
	ctx = namespaces.WithNamespace(ctx, ns)

	defers := make([]func() error, 0)
	cleanup := func() error {
		var err error
		for i := range defers {
			err = multierror.Append(err, defers[len(defers)-1-i]()).ErrorOrNil()
		}
		return err
	}
	defer func() {
		if rerr != nil && cleanup != nil {
			cleanup()
		}
	}()

	tmpdir, err := ioutil.TempDir("", "buildkit-test")
	if err != nil {
		return nil, nil, nil, err
	}
	defers = append(defers, func() error {
		return os.RemoveAll(tmpdir)
	})

	var ctdSnapshotter snapshots.Snapshotter
	var noHardlink bool
	switch snapshotterName {
	case "native-nohardlink":
		noHardlink = true
		fallthrough
	case "native":
		ctdSnapshotter, err = native.NewSnapshotter(filepath.Join(tmpdir, "snapshots"))
		if err != nil {
			return nil, nil, nil, err
		}
	case "overlayfs":
		ctdSnapshotter, err = overlay.NewSnapshotter(filepath.Join(tmpdir, "snapshots"))
		if err != nil {
			return nil, nil, nil, err
		}
	default:
		return nil, nil, nil, fmt.Errorf("unhandled snapshotter: %s", snapshotterName)
	}

	store, err := local.NewStore(tmpdir)
	if err != nil {
		return nil, nil, nil, err
	}

	db, err := bolt.Open(filepath.Join(tmpdir, "containerdmeta.db"), 0644, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	defers = append(defers, func() error {
		return db.Close()
	})

	mdb := ctdmetadata.NewDB(db, store, map[string]snapshots.Snapshotter{
		snapshotterName: ctdSnapshotter,
	})
	if err := mdb.Init(context.TODO()); err != nil {
		return nil, nil, nil, err
	}

	lm := leaseutil.WithNamespace(ctdmetadata.NewLeaseManager(mdb), ns)
	c := mdb.ContentStore()
	applier := winlayers.NewFileSystemApplierWithWindows(c, apply.NewFileSystemApplier(c))
	differ := winlayers.NewWalkingDiffWithWindows(c, walking.NewWalkingDiff(c))
	snapshotter := FromContainerdSnapshotter(ctx,
		snapshotterName,
		mdb.Snapshotter(snapshotterName),
		nil, lm, applier, differ,
	).(*fromContainerd)
	if noHardlink {
		snapshotter.useHardlinks = false
	}

	leaseID := identity.NewID()
	_, err = lm.Create(ctx, func(l *leases.Lease) error {
		l.ID = leaseID
		l.Labels = map[string]string{
			"containerd.io/gc.flat": time.Now().UTC().Format(time.RFC3339Nano),
		}
		return nil
	}, leaseutil.MakeTemporary)
	if err != nil {
		return nil, nil, nil, err
	}
	ctx = leases.WithLease(ctx, leaseID)

	return ctx, snapshotter, cleanup, nil
}

func TestStat(t *testing.T) {
	for _, snName := range []string{"native", "native-nohardlink", "overlayfs"} {
		snName := snName
		t.Run(snName, func(t *testing.T) {
			t.Parallel()

			ctx, sn, cleanup, err := NewSnapshotter(context.Background(), snName)
			require.NoError(t, err)
			defer cleanup()

			// make pre-existing keys to test backwards compatibility with old buildkit installations
			preExistingKey1 := identity.NewID()
			_, err = sn.Containerd().Prepare(ctx, preExistingKey1, "")
			require.NoError(t, err)
			preExistingKey2 := identity.NewID()
			_, err = sn.Containerd().Prepare(ctx, preExistingKey2, "")
			require.NoError(t, err)
			preExistingKey3 := identity.NewID()
			err = sn.Containerd().Commit(ctx, preExistingKey3, preExistingKey2)
			require.NoError(t, err)

			info, err := sn.Stat(ctx, preExistingKey1)
			require.NoError(t, err)
			require.Equal(t, info.Name, preExistingKey1)
			require.Equal(t, info.Kind, snapshots.KindActive)
			require.Equal(t, info.Parent, "")

			info, err = sn.Stat(ctx, preExistingKey3)
			require.NoError(t, err)
			require.Equal(t, info.Name, preExistingKey3)
			require.Equal(t, info.Kind, snapshots.KindCommitted)
			require.Equal(t, info.Parent, "")

			// test new keys have expected format
			newKey1 := identity.NewID()
			info, err = sn.Stat(ctx, newKey1)
			require.Error(t, err)
			err = sn.Prepare(ctx, newKey1, "")
			require.NoError(t, err)
			for _, k := range []string{newKey1, sn.commitKey(newKey1), sn.activeKey(newKey1)} {
				info, err = sn.Stat(ctx, k)
				require.NoError(t, err)
				require.Equal(t, info.Name, sn.activeKey(k))
				require.Equal(t, info.Kind, snapshots.KindActive)
				require.Equal(t, info.Parent, "")
			}

			err = sn.Commit(ctx, newKey1)
			require.NoError(t, err)
			for _, k := range []string{newKey1, sn.commitKey(newKey1), sn.activeKey(newKey1)} {
				info, err = sn.Stat(ctx, k)
				require.NoError(t, err)
				require.Equal(t, info.Name, sn.commitKey(k))
				require.Equal(t, info.Kind, snapshots.KindCommitted)
				require.Equal(t, info.Parent, "")
			}

			newKey2 := identity.NewID()
			info, err = sn.Stat(ctx, newKey2)
			require.Error(t, err)
			err = sn.Prepare(ctx, newKey2, newKey1)
			require.NoError(t, err)
			for _, k := range []string{newKey2, sn.commitKey(newKey2), sn.activeKey(newKey2)} {
				info, err = sn.Stat(ctx, k)
				require.NoError(t, err)
				require.Equal(t, info.Name, sn.activeKey(k))
				require.Equal(t, info.Kind, snapshots.KindActive)
				require.Equal(t, info.Parent, sn.commitKey(newKey1))
			}

			err = sn.Commit(ctx, newKey2)
			require.NoError(t, err)
			info, err = sn.Stat(ctx, newKey2)
			require.NoError(t, err)
			for _, k := range []string{newKey2, sn.commitKey(newKey2), sn.activeKey(newKey2)} {
				info, err = sn.Stat(ctx, k)
				require.NoError(t, err)
				require.Equal(t, info.Name, sn.commitKey(k))
				require.Equal(t, info.Kind, snapshots.KindCommitted)
				require.Equal(t, info.Parent, sn.commitKey(newKey1))
			}

			newKey3 := identity.NewID()
			info, err = sn.Stat(ctx, newKey3)
			require.Error(t, err)
			err = sn.Prepare(ctx, newKey3, preExistingKey3)
			require.NoError(t, err)
			for _, k := range []string{newKey3, sn.commitKey(newKey3), sn.activeKey(newKey3)} {
				info, err = sn.Stat(ctx, k)
				require.NoError(t, err)
				require.Equal(t, info.Name, sn.activeKey(k))
				require.Equal(t, info.Kind, snapshots.KindActive)
				require.Equal(t, info.Parent, preExistingKey3)
			}

			err = sn.Commit(ctx, newKey3)
			require.NoError(t, err)
			info, err = sn.Stat(ctx, newKey3)
			require.NoError(t, err)
			for _, k := range []string{newKey3, sn.commitKey(newKey3), sn.activeKey(newKey3)} {
				info, err = sn.Stat(ctx, k)
				require.NoError(t, err)
				require.Equal(t, info.Name, sn.commitKey(k))
				require.Equal(t, info.Kind, snapshots.KindCommitted)
				require.Equal(t, info.Parent, preExistingKey3)
			}
		})
	}
}

func TestMergeCleanup(t *testing.T) {
	t.Parallel()

	dir1, err := ioutil.TempDir("", "buildkit-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir1)

	err = os.MkdirAll(filepath.Join(dir1, "exists"), 0700)
	require.NoError(t, err)

	dir2, err := ioutil.TempDir("", "buildkit-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)

	merged := mergedOverlay{parents: []mount.Mount{
		{
			Type:    "bind",
			Source:  dir1,
			Options: []string{"bind"},
		},
		{
			Type:    "bind",
			Source:  dir2,
			Options: []string{"bind"},
		},
	}, userxattr: true}

	mnts, cleanup, err := merged.Mount()
	require.NoError(t, err)
	require.Len(t, mnts, 1)
	mnt := mnts[0]
	// verify an overlay was constructed w/ temp fs and work subdirs on the rw bindmount
	require.Equal(t, "overlay", mnt.Type)
	require.Contains(t, mnt.Options, "lowerdir="+dir1)
	require.Contains(t, mnt.Options, "workdir="+filepath.Join(dir2, "/work"))
	var upperdir string
	for _, opt := range mnt.Options {
		if strings.HasPrefix(opt, "upperdir=") {
			upperdir = strings.Split(opt, "=")[1]
		}
	}
	require.NotEmpty(t, upperdir)

	// create an opaque dir that overrides a directory below it
	exists := filepath.Join(upperdir, "exists")
	err = os.MkdirAll(exists, 0666)
	require.NoError(t, err)
	err = unix.Lsetxattr(exists, userOpaqueXattr, []byte("y"), 0)
	require.NoError(t, err)

	// create an opaque dir that doesn't override a directory below it (cleanup should thus remove it)
	notExists := filepath.Join(upperdir, "notexists")
	err = os.MkdirAll(notExists, 0700)
	require.NoError(t, err)
	err = unix.Lsetxattr(notExists, userOpaqueXattr, []byte("y"), 0)
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)

	// Verify that the fs temp dir was fixed by cleanup and that xattrs were kept/removed as expected.
	_, err = unix.Lgetxattr(filepath.Join(dir2, "exists"), userOpaqueXattr, make([]byte, 1))
	require.NoError(t, err)

	_, err = unix.Lgetxattr(filepath.Join(dir2, "notexists"), userOpaqueXattr, make([]byte, 1))
	require.Equal(t, unix.ENODATA, err)
}

func TestDeduplicateMounts(t *testing.T) {
	for _, snName := range []string{"overlayfs"} {
		snName := snName
		t.Run(snName, func(t *testing.T) {
			t.Parallel()

			ctx, sn, cleanup, err := NewSnapshotter(context.Background(), snName)
			require.NoError(t, err)
			defer cleanup()

			baseKey := identity.NewID()
			err = sn.Prepare(ctx, baseKey, "")
			require.NoError(t, err)
			err = sn.Commit(ctx, baseKey)
			require.NoError(t, err)
			baseMntable, err := sn.Mounts(ctx, baseKey)
			require.NoError(t, err)
			baseMnts, cleanup, err := baseMntable.Mount()
			require.NoError(t, err)
			require.Len(t, baseMnts, 1)
			baseMnt := baseMnts[0]
			require.Equal(t, "bind", baseMnt.Type)
			baseDir := baseMnt.Source
			err = cleanup()
			require.NoError(t, err)

			child1Key := identity.NewID()
			err = sn.Prepare(ctx, child1Key, baseKey)
			require.NoError(t, err)
			child1Mntable, err := sn.Mounts(ctx, child1Key)
			require.NoError(t, err)
			child1Mnts, cleanup, err := child1Mntable.Mount()
			require.NoError(t, err)
			require.Len(t, child1Mnts, 1)
			child1Mnt := child1Mnts[0]
			require.Equal(t, "overlay", child1Mnt.Type)
			var child1Dir string
			for _, opt := range child1Mnt.Options {
				if strings.HasPrefix(opt, "upperdir=") {
					child1Dir = strings.Split(opt, "=")[1]
				}
			}
			require.NotEmpty(t, child1Dir)
			err = cleanup()
			require.NoError(t, err)

			child2Key := identity.NewID()
			err = sn.Prepare(ctx, child2Key, baseKey)
			require.NoError(t, err)
			err = sn.Commit(ctx, child2Key)
			require.NoError(t, err)
			child2Mntable, err := sn.Mounts(ctx, child2Key)
			require.NoError(t, err)
			child2Mnts, cleanup, err := child2Mntable.Mount()
			require.NoError(t, err)
			require.Len(t, child2Mnts, 1)
			child2Mnt := child2Mnts[0]
			var child2Dir string
			require.Equal(t, "overlay", child2Mnt.Type)
			for _, opt := range child2Mnt.Options {
				if strings.HasPrefix(opt, "lowerdir=") {
					child2Dir = strings.Split(strings.Split(opt, "=")[1], ":")[0]
				}
			}
			require.NotEmpty(t, child2Dir)
			err = cleanup()
			require.NoError(t, err)

			merge1Key := identity.NewID()
			err = sn.Merge(ctx, merge1Key, []string{child2Key, child1Key})
			require.NoError(t, err)

			mergeMntable, err := sn.Mounts(ctx, merge1Key)
			require.NoError(t, err)
			mergeMnts, cleanup, err := mergeMntable.Mount()
			require.NoError(t, err)
			defer cleanup()
			require.Len(t, mergeMnts, 1)
			mergeMnt := mergeMnts[0]
			require.Equal(t, "overlay", mergeMnt.Type)
			require.Contains(t, mergeMnt.Options, "lowerdir="+strings.Join([]string{baseDir, child2Dir}, ":"))
			require.Contains(t, mergeMnt.Options, "upperdir="+child1Dir)

			merge2Key := identity.NewID()
			err = sn.Merge(ctx, merge2Key, []string{child1Key, child2Key})
			require.NoError(t, err)

			mergeMntable, err = sn.Mounts(ctx, merge2Key)
			require.NoError(t, err)
			mergeMnts, cleanup, err = mergeMntable.Mount()
			require.NoError(t, err)
			defer cleanup()
			require.Len(t, mergeMnts, 1)
			mergeMnt = mergeMnts[0]
			require.Equal(t, "overlay", mergeMnt.Type)
			require.Contains(t, mergeMnt.Options, "lowerdir="+strings.Join([]string{child2Dir, baseDir, child1Dir}, ":"))

			merge3Key := identity.NewID()
			err = sn.Merge(ctx, merge3Key, []string{merge2Key, merge1Key})
			require.NoError(t, err)

			mergeMntable, err = sn.Mounts(ctx, merge3Key)
			require.NoError(t, err)
			mergeMnts, cleanup, err = mergeMntable.Mount()
			require.NoError(t, err)
			defer cleanup()
			require.Len(t, mergeMnts, 1)
			mergeMnt = mergeMnts[0]
			require.Equal(t, "overlay", mergeMnt.Type)
			require.Contains(t, mergeMnt.Options, "lowerdir="+strings.Join([]string{baseDir, child2Dir}, ":"))
			require.Contains(t, mergeMnt.Options, "upperdir="+child1Dir)
		})
	}
}

/* TODO: coverage
* deduplicating mounts in the inefficient snapshotter and in the cache package
* branch from a merge, change something in the underlying merge and make sure the other branch isn't affected
* unit test: case where fs/ already exists in rw bind mount that is merged
* e2e test: assert on dir+file times
* hardlinks in inefficient cases
* sockets+fifos
 */
