// +build linux

package snapshot

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/containerd/content/local"
	"github.com/containerd/containerd/leases"
	ctdmetadata "github.com/containerd/containerd/metadata"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/native"
	"github.com/containerd/containerd/snapshots/overlay"
	"github.com/containerd/continuity/fs/fstest"
	"github.com/hashicorp/go-multierror"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/util/leaseutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

func newSnapshotter(ctx context.Context, snapshotterName string) (_ context.Context, _ *fromContainerd, _ func() error, rerr error) {
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
	snapshotter := FromContainerdSnapshotter(ctx, snapshotterName, mdb.Snapshotter(snapshotterName), nil, lm, false).(*fromContainerd)
	if noHardlink {
		snapshotter.useHardlinks = false
	}
	if snapshotterName == "overlayfs" {
		snapshotter.userxattr = true
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

func TestMerge(t *testing.T) {
	// overlayfs is tested in e2e client_test.go
	for _, snName := range []string{"native", "native-nohardlink"} {
		snName := snName
		t.Run(snName, func(t *testing.T) {
			t.Parallel()

			ctx, sn, cleanup, err := newSnapshotter(context.Background(), snName)
			require.NoError(t, err)
			defer cleanup()

			ts := time.Unix(0, 0)
			snapA := committedKey(ctx, t, sn, identity.NewID(), "",
				fstest.CreateFile("foo", []byte("A"), 0777),
				fstest.Lchtimes("foo", ts, ts.Add(2*time.Second)),

				fstest.CreateFile("a", []byte("A"), 0777),
				fstest.Lchtimes("a", ts, ts.Add(4*time.Second)),

				fstest.CreateDir("bar", 0700),
				fstest.CreateFile("bar/A", []byte("A"), 0777),
				fstest.Lchtimes("bar/A", ts, ts.Add(6*time.Second)),
				fstest.Lchtimes("bar", ts, ts.Add(6*time.Second)),
			)
			snapB := committedKey(ctx, t, sn, identity.NewID(), snapA.Name,
				fstest.Remove("/foo"),

				fstest.CreateFile("b", []byte("B"), 0777),
				fstest.Lchtimes("b", ts, ts.Add(4*time.Second)),

				fstest.CreateFile("bar/B", []byte("B"), 0774),
				fstest.Lchtimes("bar/B", ts, ts.Add(9*time.Second)),
				fstest.Lchtimes("bar", ts, ts.Add(9*time.Second)),
			)
			snapC := committedKey(ctx, t, sn, identity.NewID(), "",
				fstest.CreateFile("foo", []byte("C"), 0775),
				fstest.Lchtimes("foo", ts, ts.Add(4*time.Second)),

				fstest.CreateFile("c", []byte("C"), 0777),
				fstest.Lchtimes("c", ts, ts.Add(6*time.Second)),

				fstest.CreateDir("bar", 0777),
				fstest.CreateFile("bar/A", []byte("C"), 0400),
				fstest.Lchtimes("bar/A", ts, ts.Add(12*time.Second)),
				fstest.Lchtimes("bar", ts, ts.Add(12*time.Second)),

				fstest.Symlink("foo", "symlink"),
				fstest.Lchtimes("symlink", ts, ts.Add(3*time.Second)),
				fstest.Link("bar/A", "hardlink"),
				fstest.Symlink("../..", "dontfollowme"),
				fstest.Lchtimes("dontfollowme", ts, ts.Add(2*time.Second)),
			)

			mergeA := mergeKey(ctx, t, sn, identity.NewID(), []string{snapB.Name, snapC.Name})
			mergeADir := getBindDir(ctx, t, sn, mergeA.Name)
			require.NoError(t, fstest.CheckDirectoryEqualWithApplier(mergeADir, fstest.Apply(
				fstest.CreateFile("a", []byte("A"), 0777),
				fstest.CreateFile("b", []byte("B"), 0777),
				fstest.CreateFile("c", []byte("C"), 0777),

				fstest.CreateFile("foo", []byte("C"), 0775),

				fstest.CreateDir("bar", 0777),
				fstest.CreateFile("bar/A", []byte("C"), 0400),
				fstest.CreateFile("bar/B", []byte("B"), 0774),

				fstest.Symlink("foo", "symlink"),
				fstest.Link("bar/A", "hardlink"),
				fstest.Symlink("../..", "dontfollowme"),
			)))
			requireMtime(t, filepath.Join(mergeADir, "a"), ts.Add(4*time.Second))
			requireMtime(t, filepath.Join(mergeADir, "b"), ts.Add(4*time.Second))
			requireMtime(t, filepath.Join(mergeADir, "c"), ts.Add(6*time.Second))
			requireMtime(t, filepath.Join(mergeADir, "foo"), ts.Add(4*time.Second))
			requireMtime(t, filepath.Join(mergeADir, "bar"), ts.Add(12*time.Second))
			requireMtime(t, filepath.Join(mergeADir, "bar/A"), ts.Add(12*time.Second))
			requireMtime(t, filepath.Join(mergeADir, "bar/B"), ts.Add(9*time.Second))
			requireMtime(t, filepath.Join(mergeADir, "symlink"), ts.Add(3*time.Second))
			requireMtime(t, filepath.Join(mergeADir, "dontfollowme"), ts.Add(2*time.Second))

			mergeB := mergeKey(ctx, t, sn, identity.NewID(), []string{snapC.Name, snapB.Name})
			mergeBDir := getBindDir(ctx, t, sn, mergeB.Name)
			require.NoError(t, fstest.CheckDirectoryEqualWithApplier(mergeBDir, fstest.Apply(
				fstest.CreateFile("a", []byte("A"), 0777),
				fstest.CreateFile("b", []byte("B"), 0777),
				fstest.CreateFile("c", []byte("C"), 0777),

				fstest.CreateDir("bar", 0700),
				fstest.CreateFile("bar/A", []byte("A"), 0777),
				fstest.CreateFile("bar/B", []byte("B"), 0774),

				fstest.Symlink("foo", "symlink"),
				fstest.CreateFile("hardlink", []byte("C"), 0400), // bar/A was overwritten, not considered hardlink
				fstest.Symlink("../..", "dontfollowme"),
			)))
			requireMtime(t, filepath.Join(mergeBDir, "a"), ts.Add(4*time.Second))
			requireMtime(t, filepath.Join(mergeBDir, "b"), ts.Add(4*time.Second))
			requireMtime(t, filepath.Join(mergeBDir, "c"), ts.Add(6*time.Second))
			requireMtime(t, filepath.Join(mergeBDir, "bar"), ts.Add(9*time.Second))
			requireMtime(t, filepath.Join(mergeBDir, "bar/A"), ts.Add(6*time.Second))
			requireMtime(t, filepath.Join(mergeBDir, "bar/B"), ts.Add(9*time.Second))
			requireMtime(t, filepath.Join(mergeBDir, "symlink"), ts.Add(3*time.Second))
			requireMtime(t, filepath.Join(mergeBDir, "dontfollowme"), ts.Add(2*time.Second))

			snapD := committedKey(ctx, t, sn, identity.NewID(), "",
				fstest.CreateDir("bar", 0750),
				fstest.CreateFile("bar/D", []byte("D"), 0444),
				fstest.CreateDir("fs", 0770),
				fstest.CreateFile("x", []byte("X"), 0400),
				fstest.Link("x", "hardlink"),
				fstest.Symlink("fs", "symlink"),
				fstest.Link("symlink", "hardsymlink"),
			)

			mergeC := mergeKey(ctx, t, sn, identity.NewID(), []string{mergeA.Name, mergeB.Name, snapD.Name})
			mergeCDir := getBindDir(ctx, t, sn, mergeC.Name)
			require.NoError(t, fstest.CheckDirectoryEqualWithApplier(mergeCDir, fstest.Apply(
				fstest.CreateFile("a", []byte("A"), 0777),
				fstest.CreateFile("b", []byte("B"), 0777),
				fstest.CreateFile("c", []byte("C"), 0777),
				fstest.CreateDir("bar", 0750),
				fstest.CreateFile("bar/A", []byte("A"), 0777),
				fstest.CreateFile("bar/B", []byte("B"), 0774),
				fstest.CreateFile("bar/D", []byte("D"), 0444),
				fstest.CreateDir("fs", 0770),
				fstest.CreateFile("x", []byte("X"), 0400),
				fstest.Link("x", "hardlink"),
				fstest.Symlink("fs", "symlink"),
				fstest.Link("symlink", "hardsymlink"),
				fstest.Symlink("../..", "dontfollowme"),
			)))

			snapE := committedKey(ctx, t, sn, identity.NewID(), mergeC.Name,
				fstest.Remove("a"),
				fstest.CreateDir("a", 0770),
				fstest.Rename("b", "a/b"),
				fstest.Rename("c", "a/c"),

				fstest.RemoveAll("bar"),
				fstest.CreateDir("bar", 0777),
				fstest.CreateFile("bar/D", []byte("D2"), 0444),

				fstest.RemoveAll("fs"),
				fstest.CreateFile("fs", nil, 0764),

				fstest.Remove("x"),
				fstest.CreateDir("x", 0555),

				fstest.Remove("hardsymlink"),
				fstest.CreateDir("hardsymlink", 0707),
			)

			mergeD := mergeKey(ctx, t, sn, identity.NewID(), []string{
				committedKey(ctx, t, sn, identity.NewID(), "", fstest.CreateFile("qaz", nil, 0444)).Name,
				snapE.Name,
			})
			mergeDDir := getBindDir(ctx, t, sn, mergeD.Name)
			require.NoError(t, fstest.CheckDirectoryEqualWithApplier(mergeDDir, fstest.Apply(
				fstest.CreateDir("a", 0770),
				fstest.CreateFile("a/b", []byte("B"), 0777),
				fstest.CreateDir("bar", 0777),
				fstest.CreateFile("a/c", []byte("C"), 0777),
				fstest.CreateFile("bar/D", []byte("D2"), 0444),
				fstest.CreateFile("fs", nil, 0764),
				fstest.CreateDir("x", 0555),
				fstest.CreateFile("hardlink", []byte("X"), 0400),
				fstest.Symlink("fs", "symlink"),
				fstest.CreateDir("hardsymlink", 0707),
				fstest.Symlink("../..", "dontfollowme"),
				fstest.CreateFile("qaz", nil, 0444),
			)))
		})
	}
}

func TestDeduplicateDiffApplyMerge(t *testing.T) {
	for _, snName := range []string{"native", "native-nohardlink"} {
		snName := snName
		t.Run(snName, func(t *testing.T) {
			t.Parallel()

			ctx, sn, cleanup, err := newSnapshotter(context.Background(), snName)
			require.NoError(t, err)
			defer cleanup()

			baseSnap := committedKey(ctx, t, sn, identity.NewID(), "")
			child1Snap := committedKey(ctx, t, sn, identity.NewID(), baseSnap.Name)
			child2Snap := committedKey(ctx, t, sn, identity.NewID(), baseSnap.Name)
			merge1Snap := mergeKey(ctx, t, sn, identity.NewID(), []string{child1Snap.Name, child2Snap.Name})
			merge2Snap := mergeKey(ctx, t, sn, identity.NewID(), []string{child1Snap.Name, child2Snap.Name})
			merge3Snap := mergeKey(ctx, t, sn, identity.NewID(), []string{child2Snap.Name, child1Snap.Name, child2Snap.Name})

			require.Equal(t, merge1Snap.Parent, merge2Snap.Parent)
			require.Equal(t, merge1Snap.Parent, merge3Snap.Parent)

			merge1Snap = mergeKey(ctx, t, sn, identity.NewID(), []string{child2Snap.Name, child1Snap.Name})
			merge2Snap = mergeKey(ctx, t, sn, identity.NewID(), []string{child2Snap.Name, child1Snap.Name})
			merge3Snap = mergeKey(ctx, t, sn, identity.NewID(), []string{child1Snap.Name, child2Snap.Name, child1Snap.Name})

			require.Equal(t, merge1Snap.Parent, merge2Snap.Parent)
			require.Equal(t, merge1Snap.Parent, merge3Snap.Parent)
		})
	}
}

func TestNativeHardlinks(t *testing.T) {
	for _, snName := range []string{"native"} {
		snName := snName
		t.Run(snName, func(t *testing.T) {
			t.Parallel()

			ctx, sn, cleanup, err := newSnapshotter(context.Background(), snName)
			require.NoError(t, err)
			defer cleanup()

			base1Snap := committedKey(ctx, t, sn, identity.NewID(), "", fstest.CreateFile("1", []byte("1"), 0600))
			base2Snap := committedKey(ctx, t, sn, identity.NewID(), "", fstest.CreateFile("2", []byte("2"), 0600))

			mergeSnap := mergeKey(ctx, t, sn, identity.NewID(), []string{base1Snap.Name, base2Snap.Name})
			require.Equal(t, snapshots.KindView, mergeSnap.Kind)
			mergeDir := getBindDir(ctx, t, sn, mergeSnap.Name)

			stat1 := syscallStat(t, filepath.Join(mergeDir, "1"))
			require.EqualValues(t, 2, stat1.Nlink)
			stat1Ino := stat1.Ino

			stat2 := syscallStat(t, filepath.Join(mergeDir, "2"))
			require.EqualValues(t, 2, stat2.Nlink)
			stat2Ino := stat2.Ino

			activeSnap := activeKey(ctx, t, sn, identity.NewID(), mergeSnap.Name)
			activeDir := getBindDir(ctx, t, sn, activeSnap.Name)

			stat1 = syscallStat(t, filepath.Join(activeDir, "1"))
			require.EqualValues(t, 1, stat1.Nlink)
			require.NotEqualValues(t, stat1Ino, stat1.Ino)

			stat2 = syscallStat(t, filepath.Join(activeDir, "2"))
			require.EqualValues(t, 1, stat2.Nlink)
			require.NotEqualValues(t, stat2Ino, stat2.Ino)
		})
	}
}

func TestFixOpaqueDirs(t *testing.T) {
	for _, snName := range []string{"overlayfs"} {
		snName := snName
		t.Run(snName, func(t *testing.T) {
			t.Parallel()

			ctx, sn, cleanup, err := newSnapshotter(context.Background(), snName)
			require.NoError(t, err)
			defer cleanup()

			baseSnap := committedKey(ctx, t, sn, identity.NewID(), "",
				fstest.CreateDir("bar", 0777),
				fstest.CreateDir("qaz", 0777),
			)
			child1Snap := committedKey(ctx, t, sn, identity.NewID(), baseSnap.Name,
				fstest.CreateDir("bar", 0777),
				fstest.SetXAttr("bar", userOpaqueXattr, "y"),
				fstest.CreateDir("qaz", 0777),
				fstest.CreateDir("foo", 0777),
				fstest.SetXAttr("foo", userOpaqueXattr, "y"),
				fstest.CreateFile("foo/1", []byte("1"), 0777),
			)
			child2Snap := committedKey(ctx, t, sn, identity.NewID(), baseSnap.Name,
				fstest.CreateDir("bar", 0777),
				fstest.SetXAttr("bar", userOpaqueXattr, "y"),
				fstest.CreateDir("qaz", 0777),
				fstest.SetXAttr("qaz", userOpaqueXattr, "y"),
				fstest.CreateDir("foo", 0777),
				fstest.SetXAttr("foo", userOpaqueXattr, "y"),
				fstest.CreateFile("foo/2", []byte("2"), 0777),
			)
			mergeSnap := mergeKey(ctx, t, sn, identity.NewID(), []string{child1Snap.Name, child2Snap.Name})
			lowerdirs, _, _ := getOverlayDirs(ctx, t, sn, mergeSnap.Name)
			require.Len(t, lowerdirs, 3)

			requireIsOpaque(t, filepath.Join(lowerdirs[0], "bar"))
			requireIsOpaque(t, filepath.Join(lowerdirs[0], "qaz"))
			requireIsNotOpaque(t, filepath.Join(lowerdirs[0], "foo"))
			require.FileExists(t, filepath.Join(lowerdirs[0], "foo", "2"))

			requireIsNotOpaque(t, filepath.Join(lowerdirs[1], "bar"))
			requireIsNotOpaque(t, filepath.Join(lowerdirs[1], "qaz"))

			requireIsOpaque(t, filepath.Join(lowerdirs[2], "bar"))
			requireIsNotOpaque(t, filepath.Join(lowerdirs[2], "qaz"))
			requireIsNotOpaque(t, filepath.Join(lowerdirs[2], "foo"))
			require.FileExists(t, filepath.Join(lowerdirs[2], "foo", "1"))

			activeSnap := activeKey(ctx, t, sn, identity.NewID(), mergeSnap.Name,
				fstest.CreateDir("brandnew", 0777),
				fstest.SetXAttr("brandnew", userOpaqueXattr, "y"),
			)
			activeDir := getRWDir(ctx, t, sn, activeSnap.Name)
			requireIsOpaque(t, filepath.Join(activeDir, "brandnew"))

			commitSnap := commitActiveKey(ctx, t, sn, identity.NewID(), activeSnap.Name)
			lowerdirs, _, _ = getOverlayDirs(ctx, t, sn, commitSnap.Name)
			require.Len(t, lowerdirs, 4)
			requireIsNotOpaque(t, filepath.Join(lowerdirs[0], "brandnew"))
		})
	}
}

func TestUsage(t *testing.T) {
	for _, snName := range []string{"overlayfs", "native", "native-nohardlink"} {
		snName := snName
		t.Run(snName, func(t *testing.T) {
			t.Parallel()

			ctx, sn, cleanup, err := newSnapshotter(context.Background(), snName)
			require.NoError(t, err)
			defer cleanup()

			const direntByteSize = 4096

			base1Snap := committedKey(ctx, t, sn, identity.NewID(), "",
				fstest.CreateDir("foo", 0777),
				fstest.CreateFile("foo/1", []byte("a"), 0777),
			)
			require.EqualValues(t, 3, base1Snap.Inodes)
			require.EqualValues(t, 3*direntByteSize, base1Snap.Size)

			base2Snap := committedKey(ctx, t, sn, identity.NewID(), "",
				fstest.CreateDir("foo", 0777),
				fstest.CreateFile("foo/2", []byte("aa"), 0777),
			)
			require.EqualValues(t, 3, base2Snap.Inodes)
			require.EqualValues(t, 3*direntByteSize, base2Snap.Size)

			base3Snap := committedKey(ctx, t, sn, identity.NewID(), "",
				fstest.CreateDir("foo", 0777),
				fstest.CreateFile("foo/3", []byte("aaa"), 0777),
				fstest.CreateFile("bar", nil, 0777),
			)
			require.EqualValues(t, 4, base3Snap.Inodes)
			require.EqualValues(t, 3*direntByteSize, base3Snap.Size)

			mergeSnap := mergeKey(ctx, t, sn, identity.NewID(), []string{base1Snap.Name, base2Snap.Name, base3Snap.Name})
			switch snName {
			case "overlayfs":
				require.EqualValues(t, 0, mergeSnap.Inodes)
				require.EqualValues(t, 0, mergeSnap.Size)
			case "native":
				// / and /foo were created/copied. Others should be hard-linked
				require.EqualValues(t, 2, mergeSnap.Inodes)
				require.EqualValues(t, 2*direntByteSize, mergeSnap.Size)
			case "native-nohardlink":
				require.EqualValues(t, 6, mergeSnap.Inodes)
				require.EqualValues(t, 5*direntByteSize, mergeSnap.Size)
			}
		})
	}
}

func syscallStat(t *testing.T, path string) *syscall.Stat_t {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Sys().(*syscall.Stat_t)
}

func isOpaque(path string) (bool, error) {
	for _, xattr := range []string{trustedOpaqueXattr, userOpaqueXattr} {
		val := make([]byte, 1)
		_, err := unix.Lgetxattr(path, xattr, val)
		if err == nil && string(val) == "y" {
			return true, nil
		}
		if !errors.Is(err, unix.ENODATA) {
			return false, errors.Wrapf(err, "path %q", path)
		}
	}
	return false, nil
}

func requireIsOpaque(t *testing.T, path string) {
	t.Helper()
	is, err := isOpaque(path)
	require.NoError(t, err)
	require.True(t, is)
}

func requireIsNotOpaque(t *testing.T, path string) {
	t.Helper()
	is, err := isOpaque(path)
	require.NoError(t, err)
	require.False(t, is)
}

func requireMtime(t *testing.T, path string, mtime time.Time) {
	t.Helper()
	info, err := os.Lstat(path)
	require.NoError(t, err)
	stat := info.Sys().(*syscall.Stat_t)
	require.Equal(t, mtime.UnixNano(), stat.Mtim.Nano())
}

type snapshotInfo struct {
	snapshots.Info
	snapshots.Usage
}

func getInfo(ctx context.Context, t *testing.T, sn *fromContainerd, key string) snapshotInfo {
	t.Helper()
	info, err := sn.Stat(ctx, key)
	require.NoError(t, err)
	usage, err := sn.Usage(ctx, key)
	require.NoError(t, err)
	return snapshotInfo{info, usage}
}

func activeKey(
	ctx context.Context,
	t *testing.T,
	sn *fromContainerd,
	key string,
	parent string,
	files ...fstest.Applier,
) snapshotInfo {
	t.Helper()

	err := sn.Prepare(ctx, key, parent)
	require.NoError(t, err)

	if len(files) > 0 {
		activeDir := getRWDir(ctx, t, sn, key)
		err = fstest.Apply(files...).Apply(activeDir)
		require.NoError(t, err)
	}

	return getInfo(ctx, t, sn, key)
}

func commitActiveKey(
	ctx context.Context,
	t *testing.T,
	sn *fromContainerd,
	name string,
	activeKey string,
) snapshotInfo {
	t.Helper()
	err := sn.Commit(ctx, name, activeKey)
	require.NoError(t, err)
	return getInfo(ctx, t, sn, name)
}

func committedKey(
	ctx context.Context,
	t *testing.T,
	sn *fromContainerd,
	key string,
	parent string,
	files ...fstest.Applier,
) snapshotInfo {
	t.Helper()
	prepareKey := identity.NewID()
	activeKey(ctx, t, sn, prepareKey, parent, files...)
	return commitActiveKey(ctx, t, sn, key, prepareKey)
}

func mergeKey(
	ctx context.Context,
	t *testing.T,
	sn *fromContainerd,
	key string,
	parents []string,
) snapshotInfo {
	t.Helper()
	err := sn.Merge(ctx, key, parents)
	require.NoError(t, err)
	return getInfo(ctx, t, sn, key)
}

func getRWDir(ctx context.Context, t *testing.T, sn *fromContainerd, key string) string {
	t.Helper()

	mntable, err := sn.Mounts(ctx, key)
	require.NoError(t, err)
	mnts, cleanup, err := mntable.Mount()
	require.NoError(t, err)
	defer cleanup()
	require.Len(t, mnts, 1)
	mnt := mnts[0]
	var activeDir string
	switch mnt.Type {
	case "bind", "rbind":
		activeDir = mnt.Source
	case "overlay":
		for _, opt := range mnt.Options {
			if strings.HasPrefix(opt, "upperdir=") {
				activeDir = strings.SplitN(opt, "upperdir=", 2)[1]
				break
			}
		}
	}
	require.NotEmpty(t, activeDir)
	return activeDir
}

func getBindDir(ctx context.Context, t *testing.T, sn *fromContainerd, key string) string {
	t.Helper()

	mntable, err := sn.Mounts(ctx, key)
	require.NoError(t, err)
	mnts, cleanup, err := mntable.Mount()
	require.NoError(t, err)
	defer cleanup()
	require.Len(t, mnts, 1)
	mnt := mnts[0]
	require.Contains(t, []string{"bind", "rbind"}, mnt.Type)
	return mnt.Source
}

func getOverlayDirs(ctx context.Context, t *testing.T, sn *fromContainerd, key string) (lowerdirs []string, upperdir string, workdir string) {
	t.Helper()

	info, err := sn.Stat(ctx, key)
	require.NoError(t, err)

	if info.Kind == snapshots.KindCommitted {
		viewID := identity.NewID()
		err = sn.View(ctx, viewID, key)
		require.NoError(t, err)
		key = viewID
	}

	mntable, err := sn.Mounts(ctx, key)
	require.NoError(t, err)
	mnts, cleanup, err := mntable.Mount()
	require.NoError(t, err)
	defer cleanup()
	require.Len(t, mnts, 1)
	mnt := mnts[0]
	require.Equal(t, "overlay", mnt.Type)
	for _, opt := range mnt.Options {
		if strings.HasPrefix(opt, "lowerdir=") {
			lowerdirs = strings.Split(strings.SplitN(opt, "lowerdir=", 2)[1], ":")
			continue
		}
		if strings.HasPrefix(opt, "upperdir=") {
			upperdir = strings.SplitN(opt, "upperdir=", 2)[1]
			continue
		}
		if strings.HasPrefix(opt, "workdir=") {
			upperdir = strings.SplitN(opt, "workdir=", 2)[1]
			continue
		}
	}
	require.NotEmpty(t, lowerdirs)
	return lowerdirs, upperdir, workdir
}
