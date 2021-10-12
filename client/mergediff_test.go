package client

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/continuity/fs/fstest"
	"github.com/moby/buildkit/client/llb"
	"github.com/moby/buildkit/util/testutil/integration"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// TODO: move merge tests here too

// contents lets you create fstest.Appliers using the sandbox, which
// enables i.e. using an llb.State or fstest.Applier interchangeably
// during test assertions on the contents of a given dir.
type contents func(sb integration.Sandbox) fstest.Applier

// implements fstest.Applier
type applyFn func(root string) error

func (a applyFn) Apply(root string) error {
	return a(root)
}

func contentsOf(state llb.State) contents {
	return func(sb integration.Sandbox) fstest.Applier {
		return applyFn(func(root string) error {
			ctx := sb.Context()
			c, err := New(ctx, sb.Address())
			if err != nil {
				return err
			}
			defer c.Close()

			def, err := state.Marshal(ctx)
			if err != nil {
				return err
			}

			_, err = c.Solve(ctx, def, SolveOpt{
				Exports: []ExportEntry{
					{
						Type:      ExporterLocal,
						OutputDir: root,
					},
				},
			}, nil)
			if err != nil {
				return err
			}
			return nil
		})
	}
}

func apply(appliers ...fstest.Applier) contents {
	return func(sb integration.Sandbox) fstest.Applier {
		return fstest.Apply(appliers...)
	}
}

func mergeContents(subContents ...contents) contents {
	return func(sb integration.Sandbox) fstest.Applier {
		var appliers []fstest.Applier
		for _, sub := range subContents {
			appliers = append(appliers, sub(sb))
		}
		return fstest.Apply(appliers...)
	}
}

func empty(sb integration.Sandbox) fstest.Applier {
	return applyFn(func(root string) error {
		return nil
	})
}

type verifyContents struct {
	name     string
	state    llb.State
	contents contents
}

func (tc verifyContents) Name() string {
	return tc.name
}

func (tc verifyContents) Run(t *testing.T, sb integration.Sandbox) {
	requiresLinux(t)
	cdAddress := sb.ContainerdAddress()

	ctx := sb.Context()
	ctdCtx := namespaces.WithNamespace(ctx, "buildkit")

	c, err := New(ctx, sb.Address())
	require.NoError(t, err)
	defer c.Close()

	registry, err := sb.NewRegistry()
	if errors.Is(err, integration.ErrorRequirements) {
		t.Skip(err.Error())
	}
	require.NoError(t, err)

	resetState := func() {
		if cdAddress != "" {
			client, err := newContainerd(cdAddress)
			require.NoError(t, err)
			defer client.Close()
			imageService := client.ImageService()
			imageList, err := imageService.List(ctdCtx)
			require.NoError(t, err)
			for _, img := range imageList {
				err = imageService.Delete(ctdCtx, img.Name, images.SynchronousDelete())
				require.NoError(t, err)
			}
		}
		checkAllReleasable(t, c, sb, true)
	}

	// verify the build has the expected contents
	resetState()
	requireContents(ctx, t, c, tc.state, nil, tc.contents(sb))

	// export as an image, verify it reimports the same
	def, err := tc.state.Marshal(sb.Context())
	require.NoError(t, err)

	imageName := fmt.Sprintf("buildkit/%s", strings.ToLower(tc.name))
	imageTarget := fmt.Sprintf("%s/%s:latest", registry, imageName)
	cacheName := fmt.Sprintf("buildkit/%s-cache", imageName)
	cacheTarget := fmt.Sprintf("%s/%s:latest", registry, cacheName)
	_, err = c.Solve(sb.Context(), def, SolveOpt{
		Exports: []ExportEntry{
			{
				Type: ExporterImage,
				Attrs: map[string]string{
					"name": imageTarget,
					"push": "true",
				},
			},
		},
		CacheExports: []CacheOptionsEntry{{
			Type: "registry",
			Attrs: map[string]string{
				"ref": cacheTarget,
			},
		}},
	}, nil)
	require.NoError(t, err)

	resetState()
	requireContents(ctx, t, c, llb.Image(imageTarget), nil, tc.contents(sb))

	// Check that the cache is actually used. This can only be asserted on
	// in containerd-based tests because it needs access to the image+content store
	if cdAddress != "" {
		client, err := newContainerd(cdAddress)
		require.NoError(t, err)
		defer client.Close()

		resetState()
		_, err = c.Solve(sb.Context(), def, SolveOpt{
			Exports: []ExportEntry{
				{
					Type: ExporterImage,
					Attrs: map[string]string{
						"name": imageTarget,
						"push": "true",
					},
				},
			},
			CacheImports: []CacheOptionsEntry{{
				Type: "registry",
				Attrs: map[string]string{
					"ref": cacheTarget,
				},
			}},
		}, nil)
		require.NoError(t, err)

		img, err := client.GetImage(ctdCtx, imageTarget)
		require.NoError(t, err)

		var unexpectedLayers []v1.Descriptor
		require.NoError(t, images.Walk(ctdCtx, images.HandlerFunc(func(ctx context.Context, desc v1.Descriptor) ([]v1.Descriptor, error) {
			if images.IsLayerType(desc.MediaType) {
				_, err := client.ContentStore().Info(ctdCtx, desc.Digest)
				if err == nil {
					unexpectedLayers = append(unexpectedLayers, desc)
				} else {
					require.True(t, errdefs.IsNotFound(err))
				}
			}
			return images.Children(ctx, client.ContentStore(), desc)
		}), img.Target()))
		require.Empty(t, unexpectedLayers)
	}

	// verify that builds using cache reimport the same contents
	resetState()
	requireContents(ctx, t, c, tc.state, []CacheOptionsEntry{{
		Type: "registry",
		Attrs: map[string]string{
			"ref": cacheTarget,
		},
	}}, tc.contents(sb))
}

func diffOpTestCases() (tests []integration.Test) {
	alpine := llb.Image("alpine:latest", llb.ResolveDigest(true))
	// busybox doesn't have /proc or /sys in its base image, add them
	// so they don't show up in every diff of an exec on it
	busybox := llb.Image("busybox:latest", llb.ResolveDigest(true)).
		File(llb.Mkdir("/proc", 0755)).
		File(llb.Mkdir("/sys", 0755))

	// Diffs of identical states are empty
	tests = append(tests,
		verifyContents{
			name:     "TestDiffScratch",
			state:    llb.Diff(llb.Scratch(), llb.Scratch()),
			contents: empty,
		},
		verifyContents{
			name:     "TestDiffSelf",
			state:    llb.Diff(alpine, alpine),
			contents: empty,
		},
		verifyContents{
			name:     "TestDiffSelfDeletes",
			state:    llb.Merge([]llb.State{alpine, llb.Diff(alpine, alpine)}),
			contents: contentsOf(alpine),
		},
	)

	// Diff of state with scratch has same contents as the state but without deletes
	tests = append(tests,
		verifyContents{
			name:     "TestDiffLowerScratch",
			state:    llb.Diff(llb.Scratch(), alpine),
			contents: contentsOf(alpine),
		},
		verifyContents{
			name: "TestDiffLowerScratchDeletes",
			state: llb.Merge([]llb.State{
				llb.Scratch().
					File(llb.Mkfile("/foo", 0777, []byte("A"))),
				llb.Diff(llb.Scratch(), llb.Scratch().
					File(llb.Mkfile("/foo", 0644, []byte("B"))).
					File(llb.Rm("/foo")).
					File(llb.Mkfile("/bar", 0644, nil))),
			}),
			contents: apply(
				fstest.CreateFile("/foo", []byte("A"), 0777),
				fstest.CreateFile("/bar", nil, 0644),
			),
		},
	)

	// Diff from a state to scratch is just a deletion of the contents of that state
	tests = append(tests,
		verifyContents{
			name: "TestDiffUpperScratch",
			state: llb.Merge([]llb.State{
				alpine,
				llb.Scratch().File(llb.Mkfile("/foo", 0644, []byte("foo"))),
				llb.Diff(alpine, llb.Scratch()),
			}),
			contents: apply(
				fstest.CreateFile("/foo", []byte("foo"), 0644),
			),
		},
	)

	// Basic diff slices
	tests = append(tests, func() (tests []integration.Test) {
		// TODO: local export resets uid/gid, so we can't test that here, make sure its in the unit tests at least

		base := alpine.
			File(llb.Mkfile("/shuffleFile1", 0644, []byte("shuffleFile1"))).
			File(llb.Mkdir("/shuffleDir1", 0755)).
			File(llb.Mkdir("/shuffleDir1/shuffleSubdir1", 0755)).
			File(llb.Mkfile("/shuffleDir1/shuffleSubfile1", 0644, nil)).
			File(llb.Mkfile("/shuffleDir1/shuffleSubdir1/shuffleSubfile2", 0644, nil)).
			File(llb.Mkdir("/unmodifiedDir", 0755)).
			File(llb.Mkdir("/unmodifiedDir/chmodSubdir1", 0755)).
			File(llb.Mkdir("/unmodifiedDir/deleteSubdir1", 0755)).
			File(llb.Mkdir("/unmodifiedDir/opaqueDir1", 0755)).
			File(llb.Mkdir("/unmodifiedDir/opaqueDir1/opaqueSubdir1", 0755)).
			File(llb.Mkdir("/unmodifiedDir/overrideSubdir1", 0755)).
			File(llb.Mkdir("/unmodifiedDir/shuffleDir2", 0755)).
			File(llb.Mkdir("/unmodifiedDir/shuffleDir2/shuffleSubdir2", 0755)).
			File(llb.Mkfile("/unmodifiedDir/chmodFile1", 0644, []byte("chmodFile1"))).
			File(llb.Mkfile("/unmodifiedDir/modifyContentFile1", 0644, []byte("modifyContentFile1"))).
			File(llb.Mkfile("/unmodifiedDir/deleteFile1", 0644, nil)).
			File(llb.Mkfile("/unmodifiedDir/opaqueDir1/opaqueFile1", 0644, nil)).
			File(llb.Mkfile("/unmodifiedDir/opaqueDir1/opaqueSubdir1/opaqueFile2", 0644, nil)).
			File(llb.Mkfile("/unmodifiedDir/overrideFile1", 0644, nil)).
			File(llb.Mkfile("/unmodifiedDir/overrideFile2", 0644, nil)).
			File(llb.Mkfile("/unmodifiedDir/shuffleFile2", 0644, []byte("shuffleFile2"))).
			File(llb.Mkfile("/unmodifiedDir/shuffleDir2/shuffleSubfile3", 0644, nil)).
			File(llb.Mkfile("/unmodifiedDir/shuffleDir2/shuffleSubdir2/shuffleSubfile4", 0644, nil)).
			File(llb.Mkdir("/modifyDir", 0755)).
			File(llb.Mkdir("/modifyDir/chmodSubdir2", 0755)).
			File(llb.Mkdir("/modifyDir/deleteSubdir2", 0755)).
			File(llb.Mkdir("/modifyDir/opaqueDir2", 0755)).
			File(llb.Mkdir("/modifyDir/opaqueDir2/opaqueSubdir2", 0755)).
			File(llb.Mkdir("/modifyDir/overrideSubdir2", 0755)).
			File(llb.Mkdir("/modifyDir/shuffleDir3", 0755)).
			File(llb.Mkdir("/modifyDir/shuffleDir3/shuffleSubdir3", 0755)).
			File(llb.Mkfile("/modifyDir/chmodFile2", 0644, []byte("chmodFile2"))).
			File(llb.Mkfile("/modifyDir/modifyContentFile2", 0644, []byte("modifyContentFile2"))).
			File(llb.Mkfile("/modifyDir/deleteFile2", 0644, nil)).
			File(llb.Mkfile("/modifyDir/opaqueDir2/opaqueFile3", 0644, nil)).
			File(llb.Mkfile("/modifyDir/opaqueDir2/opaqueSubdir2/opaqueFile4", 0644, nil)).
			File(llb.Mkfile("/modifyDir/overrideFile3", 0644, nil)).
			File(llb.Mkfile("/modifyDir/overrideFile4", 0644, nil)).
			File(llb.Mkfile("/modifyDir/shuffleFile3", 0644, []byte("shuffleFile3"))).
			File(llb.Mkfile("/modifyDir/shuffleDir3/shuffleSubfile4", 0644, nil)).
			File(llb.Mkfile("/modifyDir/shuffleDir3/shuffleSubdir3/shuffleSubfile6", 0644, nil))

		joinCmds := func(cmds ...[]string) []string {
			var all []string
			for _, cmd := range cmds {
				all = append(all, cmd...)
			}
			return all
		}

		var allCmds []string
		var allContents []contents

		baseDiffCmds := []string{
			"chmod 0700 /modifyDir",
		}
		baseDiffContents := apply(
			fstest.CreateDir("/unmodifiedDir", 0755),
			fstest.CreateDir("/modifyDir", 0700),
		)
		allCmds = append(allCmds, baseDiffCmds...)
		allContents = append(allContents, baseDiffContents)

		newFileCmds := []string{
			// Create a new file under a new dir
			"mkdir /newdir1",
			"touch /newdir1/newfile1",
			// Create a new file under an unmodified existing dir
			"touch /unmodifiedDir/newfile2",
			// Create a new file under a modified existing dir
			"touch /modifyDir/newfile3",
		}
		newFileContents := apply(
			fstest.CreateDir("/newdir1", 0755),
			fstest.CreateFile("/newdir1/newfile1", nil, 0644),
			fstest.CreateFile("/unmodifiedDir/newfile2", nil, 0644),
			fstest.CreateFile("/modifyDir/newfile3", nil, 0644),
		)
		allCmds = append(allCmds, newFileCmds...)
		allContents = append(allContents, newFileContents)
		tests = append(tests, verifyContents{
			name: "TestDiffNewFiles",
			state: llb.Diff(base, runShell(base, joinCmds(
				baseDiffCmds,
				newFileCmds,
			)...)),
			contents: mergeContents(
				baseDiffContents,
				newFileContents,
			),
		})

		modifyFileCmds := []string{
			// Modify an existing file under an unmodified existing dir
			"chmod 0444 /unmodifiedDir/chmodFile1",
			"echo -n modifyContentFile0 > /unmodifiedDir/modifyContentFile1",

			// Modify an existing file under a modified existing dir
			"chmod 0440 /modifyDir/chmodFile2",
			"echo -n modifyContentFile0 > /modifyDir/modifyContentFile2",
		}
		modifyFileContents := apply(
			fstest.CreateFile("/unmodifiedDir/chmodFile1", []byte("chmodFile1"), 0444),
			fstest.CreateFile("/unmodifiedDir/modifyContentFile1", []byte("modifyContentFile0"), 0644),

			fstest.CreateFile("/modifyDir/chmodFile2", []byte("chmodFile2"), 0440),
			fstest.CreateFile("/modifyDir/modifyContentFile2", []byte("modifyContentFile0"), 0644),
		)
		allCmds = append(allCmds, modifyFileCmds...)
		allContents = append(allContents, modifyFileContents)
		tests = append(tests, verifyContents{
			name: "TestDiffModifiedFiles",
			state: llb.Diff(base, runShell(base, joinCmds(
				baseDiffCmds,
				modifyFileCmds,
			)...)),
			contents: mergeContents(
				baseDiffContents,
				modifyFileContents,
			),
		})

		createNewDirCmds := []string{
			// Create a new dir under a new dir
			"mkdir -p /newdir2/newsubdir1",

			// Create a new dir under an unmodified existing dir
			"mkdir /unmodifiedDir/newsubdir2",

			// Create a new dir under a modified existing dir
			"mkdir /modifyDir/newsubdir3",
		}
		createNewDirContents := apply(
			fstest.CreateDir("/newdir2", 0755),
			fstest.CreateDir("/newdir2/newsubdir1", 0755),
			fstest.CreateDir("/unmodifiedDir/newsubdir2", 0755),
			fstest.CreateDir("/modifyDir/newsubdir3", 0755),
		)
		allCmds = append(allCmds, createNewDirCmds...)
		allContents = append(allContents, createNewDirContents)
		tests = append(tests, verifyContents{
			name: "TestDiffNewDirs",
			state: llb.Diff(base, runShell(base, joinCmds(
				baseDiffCmds,
				createNewDirCmds,
			)...)),
			contents: mergeContents(
				baseDiffContents,
				createNewDirContents,
			),
		})

		modifyDirCmds := []string{
			// Modify a dir under an unmodified existing dir
			"chmod 0700 /unmodifiedDir/chmodSubdir1",

			// Modify a dir under an modified existing dir
			"chmod 0770 /modifyDir/chmodSubdir2",
		}
		modifyDirContents := apply(
			fstest.CreateDir("/unmodifiedDir/chmodSubdir1", 0700),
			fstest.CreateDir("/modifyDir/chmodSubdir2", 0770),
		)
		allCmds = append(allCmds, modifyDirCmds...)
		allContents = append(allContents, modifyDirContents)
		tests = append(tests, verifyContents{
			name: "TestDiffModifiedDirs",
			state: llb.Diff(base, runShell(base, joinCmds(
				baseDiffCmds,
				modifyDirCmds,
			)...)),
			contents: mergeContents(
				baseDiffContents,
				modifyDirContents,
			),
		})

		overrideDirCmds := []string{
			"rm -rf /unmodifiedDir/overrideSubdir1",
			"echo -n overrideSubdir1 > /unmodifiedDir/overrideSubdir1",

			"rm -rf /modifyDir/overrideSubdir2",
			"echo -n overrideSubdir2 > /modifyDir/overrideSubdir2",
		}
		overrideDirContents := apply(
			fstest.CreateFile("/unmodifiedDir/overrideSubdir1", []byte("overrideSubdir1"), 0644),
			fstest.CreateFile("/modifyDir/overrideSubdir2", []byte("overrideSubdir2"), 0644),
		)
		allCmds = append(allCmds, overrideDirCmds...)
		allContents = append(allContents, overrideDirContents)
		tests = append(tests, verifyContents{
			name: "TestDiffOverrideDirs",
			state: llb.Diff(base, runShell(base, joinCmds(
				baseDiffCmds,
				overrideDirCmds,
			)...)),
			contents: mergeContents(
				baseDiffContents,
				overrideDirContents,
			),
		})

		overrideFileCmds := []string{
			"rm /unmodifiedDir/overrideFile1",
			"mkdir -m 0700 /unmodifiedDir/overrideFile1",
			"rm /unmodifiedDir/overrideFile2",
			"mkdir -m 0750 /unmodifiedDir/overrideFile2",
			"mkdir -m 0770 /unmodifiedDir/overrideFile2/newsubdir4",
			"touch /unmodifiedDir/overrideFile2/newfile4",
			"touch /unmodifiedDir/overrideFile2/newsubdir4/newfile5",

			"rm /modifyDir/overrideFile3",
			"mkdir -m 0700 /modifyDir/overrideFile3",
			"rm /modifyDir/overrideFile4",
			"mkdir -m 0750 /modifyDir/overrideFile4",
			"mkdir -m 0770 /modifyDir/overrideFile4/newsubdir5",
			"touch /modifyDir/overrideFile4/newfile6",
			"touch /modifyDir/overrideFile4/newsubdir5/newfile7",
		}
		overrideFileContents := apply(
			fstest.CreateDir("/unmodifiedDir/overrideFile1", 0700),
			fstest.CreateDir("/unmodifiedDir/overrideFile2", 0750),
			fstest.CreateDir("/unmodifiedDir/overrideFile2/newsubdir4", 0770),
			fstest.CreateFile("/unmodifiedDir/overrideFile2/newfile4", nil, 0644),
			fstest.CreateFile("/unmodifiedDir/overrideFile2/newsubdir4/newfile5", nil, 0644),

			fstest.CreateDir("/modifyDir/overrideFile3", 0700),
			fstest.CreateDir("/modifyDir/overrideFile4", 0750),
			fstest.CreateDir("/modifyDir/overrideFile4/newsubdir5", 0770),
			fstest.CreateFile("/modifyDir/overrideFile4/newfile6", nil, 0644),
			fstest.CreateFile("/modifyDir/overrideFile4/newsubdir5/newfile7", nil, 0644),
		)
		allCmds = append(allCmds, overrideFileCmds...)
		allContents = append(allContents, overrideFileContents)
		tests = append(tests, verifyContents{
			name: "TestDiffOverrideFiles",
			state: llb.Diff(base, runShell(base, joinCmds(
				baseDiffCmds,
				overrideFileCmds,
			)...)),
			contents: mergeContents(
				baseDiffContents,
				overrideFileContents,
			),
		})

		// TODO: incorporate this comment into below
		// Surprisingly, it's expected that /bin shows up in the diff
		// even though the only change was that /bin/cat was removed.
		// This is the behavior of the exporter, so we have to enforce
		// consistency with it.
		// https://github.com/containerd/containerd/pull/2095

		deleteFileCmds := []string{
			"rm /unmodifiedDir/deleteFile1",
			"rm /modifyDir/deleteFile2",
		}
		allCmds = append(allCmds, deleteFileCmds...)
		tests = append(tests, verifyContents{
			name: "TestDiffDeleteFiles",
			state: llb.Diff(base, runShell(base, joinCmds(
				baseDiffCmds,
				deleteFileCmds,
			)...)),
			contents: mergeContents(
				baseDiffContents,
			),
		})
		tests = append(tests, verifyContents{
			name: "TestDiffDeleteFilesMerge",
			state: llb.Merge([]llb.State{
				base,
				llb.Diff(base, runShell(base, joinCmds(
					baseDiffCmds,
					deleteFileCmds,
				)...)),
			}),
			contents: mergeContents(
				contentsOf(base),
				baseDiffContents,
				apply(
					fstest.Remove("/unmodifiedDir/deleteFile1"),
					fstest.Remove("/modifyDir/deleteFile2"),
				),
			),
		})

		deleteDirCmds := []string{
			"rm -rf /unmodifiedDir/deleteSubdir1",
			"rm -rf /modifyDir/deleteSubdir2",
		}
		allCmds = append(allCmds, deleteDirCmds...)
		tests = append(tests, verifyContents{
			name: "TestDiffDeleteDirs",
			state: llb.Diff(base, runShell(base, joinCmds(
				baseDiffCmds,
				deleteDirCmds,
			)...)),
			contents: mergeContents(
				baseDiffContents,
			),
		})
		tests = append(tests, verifyContents{
			name: "TestDiffDeleteDirsMerge",
			state: llb.Merge([]llb.State{
				base,
				llb.Diff(base, runShell(base, joinCmds(
					baseDiffCmds,
					deleteDirCmds,
				)...)),
			}),
			contents: mergeContents(
				contentsOf(base),
				baseDiffContents,
				apply(
					fstest.RemoveAll("/unmodifiedDir/deleteSubdir1"),
					fstest.RemoveAll("/modifyDir/deleteSubdir2"),
				),
			),
		})

		// TODO: can you trick the archive unpacked into creating a whiteout under a subdir where it doesn't apply
		// and the subdir only exists because of that whiteout? If so, that's a bug in the archiver.

		// Opaque dirs should be converted to the "explicit whiteout" format, as described in the OCI image spec:
		// https://github.com/opencontainers/image-spec/blob/main/layer.md#opaque-whiteout
		// TODO: verify that hardlinks are used when switching to double-walking? Ideally assert in test case, but at min verify it.
		opaqueDirCmds := []string{
			"rm -rf /unmodifiedDir/opaqueDir1",
			"mkdir -p /unmodifiedDir/opaqueDir1/newOpaqueSubdir1",
			"touch /unmodifiedDir/opaqueDir1/newOpaqueFile1",
			"touch /unmodifiedDir/opaqueDir1/newOpaqueSubdir1/newOpaqueFile2",

			"rm -rf /modifyDir/opaqueDir2",
			"mkdir -p /modifyDir/opaqueDir2/newOpaqueSubdir2",
			"touch /modifyDir/opaqueDir2/newOpaqueFile3",
			"touch /modifyDir/opaqueDir2/newOpaqueSubdir2/newOpaqueFile4",
		}
		opaqueDirContents := apply(
			fstest.CreateDir("/unmodifiedDir/opaqueDir1", 0755),
			fstest.CreateDir("/unmodifiedDir/opaqueDir1/newOpaqueSubdir1", 0755),
			fstest.CreateFile("/unmodifiedDir/opaqueDir1/newOpaqueFile1", nil, 0644),
			fstest.CreateFile("/unmodifiedDir/opaqueDir1/newOpaqueSubdir1/newOpaqueFile2", nil, 0644),

			fstest.CreateDir("/modifyDir/opaqueDir2", 0755),
			fstest.CreateDir("/modifyDir/opaqueDir2/newOpaqueSubdir2", 0755),
			fstest.CreateFile("/modifyDir/opaqueDir2/newOpaqueFile3", nil, 0644),
			fstest.CreateFile("/modifyDir/opaqueDir2/newOpaqueSubdir2/newOpaqueFile4", nil, 0644),
		)
		allCmds = append(allCmds, opaqueDirCmds...)
		allContents = append(allContents, opaqueDirContents)
		tests = append(tests, verifyContents{
			name: "TestDiffOpaqueDirs",
			state: llb.Diff(base, runShell(base, joinCmds(
				baseDiffCmds,
				opaqueDirCmds,
			)...)),
			contents: mergeContents(
				baseDiffContents,
				opaqueDirContents,
			),
		})
		tests = append(tests, verifyContents{
			name: "TestDiffOpaqueDirsMerge",
			state: llb.Merge([]llb.State{
				base.
					File(llb.Mkfile("/unmodifiedDir/opaqueDir1/rebaseFile1", 0644, nil)).
					File(llb.Mkfile("/unmodifiedDir/opaqueDir1/opaqueSubdir1/rebaseFile2", 0644, nil)).
					File(llb.Mkfile("/modifyDir/opaqueDir2/rebaseFile3", 0644, nil)).
					File(llb.Mkfile("/modifyDir/opaqueDir2/opaqueSubdir2/rebaseFile4", 0644, nil)),
				llb.Diff(base, runShell(base, joinCmds(
					baseDiffCmds,
					opaqueDirCmds,
				)...)),
			}),
			contents: mergeContents(
				contentsOf(base),
				baseDiffContents,
				apply(
					fstest.RemoveAll("/unmodifiedDir/opaqueDir1"),
					fstest.RemoveAll("/modifyDir/opaqueDir2"),
				),
				opaqueDirContents,
				apply(
					fstest.CreateFile("/unmodifiedDir/opaqueDir1/rebaseFile1", nil, 0644),
					fstest.CreateFile("/modifyDir/opaqueDir2/rebaseFile3", nil, 0644),
				),
			),
		})

		// TODO: comment about shuffle tests

		shuffleFileCmds := []string{
			// Shuffle a file under root
			"mv /shuffleFile1 /shuffleFile1tmp",
			"mv /shuffleFile1tmp /shuffleFile1",

			// Shuffle a file under an unmodified existing dir
			"mv /unmodifiedDir/shuffleFile2 /unmodifiedDir/shuffleFile2tmp",
			"mv /unmodifiedDir/shuffleFile2tmp /unmodifiedDir/shuffleFile2",

			// Shuffle a file under a modified existing dir
			"mv /modifyDir/shuffleFile3 /modifyDir/shuffleFile3tmp",
			"mv /modifyDir/shuffleFile3tmp /modifyDir/shuffleFile3",
		}
		allCmds = append(allCmds, shuffleFileCmds...)
		tests = append(tests, verifyContents{
			name: "TestDiffShuffleFiles",
			state: llb.Diff(base, runShell(base, joinCmds(
				baseDiffCmds,
				shuffleFileCmds,
			)...)),
			contents: mergeContents(
				baseDiffContents,
				apply(fstest.RemoveAll("/unmodifiedDir")),
			),
		})
		tests = append(tests, verifyContents{
			name: "TestDiffShuffleFilesMerge",
			state: llb.Merge([]llb.State{
				base,
				llb.Diff(base, runShell(base, joinCmds(
					baseDiffCmds,
					shuffleFileCmds,
				)...)),
			}),
			contents: mergeContents(
				contentsOf(base),
				baseDiffContents,
			),
		})

		// NOTE: this test seems to be impacted by a kernel bug (or at least very
		// unexpected behavior). It consistently fails when using overlay on:
		// "5.10.0-9-arm64 #1 SMP Debian 5.10.70-1 (2021-09-30) aarch64 GNU/Linux"
		// The failure is due to the files under shuffle dirs all having Mtim nanosecond
		// fields truncated to zero when copied up to the overlay's upperdir. This does
		// not happen in other tests and does not happen on any other kernels I've tested
		// so far.
		shuffleDirCmds := []string{
			// Shuffle a dir under root
			"mv /shuffleDir1 /shuffleDir1tmp",
			"mv /shuffleDir1tmp /shuffleDir1",

			// Shuffle a dir under an unmodified existing dir
			"mv /unmodifiedDir/shuffleDir2 /unmodifiedDir/shuffleDir2tmp",
			"mv /unmodifiedDir/shuffleDir2tmp /unmodifiedDir/shuffleDir2",

			// Shuffle a dir under a modified existing dir
			"mv /modifyDir/shuffleDir3 /modifyDir/shuffleDir3tmp",
			"mv /modifyDir/shuffleDir3tmp /modifyDir/shuffleDir3",
		}
		allCmds = append(allCmds, shuffleDirCmds...)
		tests = append(tests, verifyContents{
			name: "TestDiffShuffleDirs",
			state: llb.Diff(base, runShell(base, joinCmds(
				baseDiffCmds,
				shuffleDirCmds,
			)...)),
			contents: mergeContents(
				baseDiffContents,
				apply(fstest.RemoveAll("/unmodifiedDir")),
			),
		})
		tests = append(tests, verifyContents{
			name: "TestDiffShuffleDirsMerge",
			state: llb.Merge([]llb.State{
				base,
				llb.Diff(base, runShell(base, joinCmds(
					baseDiffCmds,
					shuffleDirCmds,
				)...)),
			}),
			contents: mergeContents(
				contentsOf(base),
				baseDiffContents,
			),
		})

		// verify that fifos and devices are handled
		mknodCmds := []string{
			"mknod /unmodifiedDir/fifo1 p",
			"mknod /modifyDir/fifo2 p",
			"mknod /unmodifiedDir/null1 c 1 3",
			"mknod /modifyDir/null2 c 1 3",
			// TODO: test sockets. Not straightforward because
			// there's no builtin commands in alpine or busybox
			// that create sockets except for daemons like udhcpd...
		}
		tests = append(tests, verifyContents{
			name: "TestDiffMknod",
			state: llb.Diff(base, runShell(base, joinCmds(
				baseDiffCmds,
				mknodCmds,
			)...)),
			contents: mergeContents(
				baseDiffContents,
				apply(
					mkfifo("/unmodifiedDir/fifo1", 0644),
					mkfifo("/modifyDir/fifo2", 0644),
					mkchardev("/unmodifiedDir/null1", 0644, 1, 3),
					mkchardev("/modifyDir/null2", 0644, 1, 3),
				),
			),
		})

		tests = append(tests, verifyContents{
			name:     "TestDiffCombinedSingleLayer",
			state:    llb.Diff(base, runShell(base, allCmds...)),
			contents: mergeContents(allContents...),
		})

		var allCmdsMultiLayer [][]string
		for _, cmds := range allCmds {
			allCmdsMultiLayer = append(allCmdsMultiLayer, []string{cmds})
		}
		tests = append(tests, verifyContents{
			name:     "TestDiffCombinedMultiLayer",
			state:    llb.Diff(base, chainRunShells(base, allCmdsMultiLayer...)),
			contents: mergeContents(allContents...),
		})
		return tests
	}()...)

	tests = append(tests, func() []integration.Test {
		base := runShell(alpine,
			"mkdir -p /parentdir/dir/subdir",
			"touch /parentdir/dir/A /parentdir/dir/B /parentdir/dir/subdir/C",
		)
		return []integration.Test{
			verifyContents{
				name: "TestDiffOpaqueDirs",
				state: llb.Merge([]llb.State{
					runShell(busybox,
						"mkdir -p /parentdir/dir/subdir",
						"touch /parentdir/dir/A /parentdir/dir/B /parentdir/dir/D",
					),
					llb.Diff(base, runShell(base,
						"rm -rf /parentdir/dir",
						"mkdir /parentdir/dir",
						"touch /parentdir/dir/E",
					)),
				}),
				contents: contentsOf(runShell(busybox,
					"mkdir -p /parentdir/dir",
					"touch /parentdir/dir/D",
					"touch /parentdir/dir/E",
				)),
			},
		}
	}()...)

	// Symlink handling tests
	tests = append(tests, func() []integration.Test {
		linkFooToBar := llb.Diff(alpine, runShell(alpine, "mkdir -p /bar", "ln -s /bar /foo"))

		alpinePlusFoo := runShell(alpine, "mkdir /foo")
		deleteFoo := llb.Diff(alpinePlusFoo, runShell(alpinePlusFoo, "rm -rf /foo"))
		createFooFile := llb.Diff(alpinePlusFoo, runShell(alpinePlusFoo, "touch /foo/file"))

		return []integration.Test{
			// TODO: explain why this case is the way it is, has to do with parent dirs always being included in tars made by exporter
			// TODO: test these cases when not directly under root too.
			verifyContents{
				name: "TestDiffDirOverridesSymlink",
				state: llb.Merge([]llb.State{
					busybox,
					linkFooToBar,
					createFooFile,
				}),
				contents: contentsOf(runShell(busybox,
					"mkdir /bar",
					"mkdir /foo",
					"touch /foo/file",
				)),
			},
			verifyContents{
				name: "TestDiffSymlinkOverridesDir",
				state: llb.Merge([]llb.State{
					busybox,
					createFooFile,
					linkFooToBar,
				}),
				contents: contentsOf(runShell(busybox,
					"mkdir /bar",
					"ln -s /bar /foo",
				)),
			},
			verifyContents{
				name: "TestDiffSymlinkOverridesSymlink",
				state: llb.Merge([]llb.State{
					busybox,
					llb.Diff(alpine, runShell(alpine,
						"mkdir /1 /2",
						"ln -s /1 /a",
						"ln -s /2 /a/b",
					)),
					llb.Diff(alpine, runShell(alpine,
						"mkdir /3",
						"ln -s /3 /a",
					)),
				}),
				contents: contentsOf(runShell(busybox,
					"mkdir /1 /2 /3",
					"ln -s /3 /a",
					"ln -s /2 /1/b",
				)),
			},

			verifyContents{
				name: "TestDiffDeleteDoesNotFollowSymlink",
				state: llb.Merge([]llb.State{
					busybox,
					linkFooToBar,
					deleteFoo,
				}),
				contents: contentsOf(runShell(busybox,
					"mkdir /bar",
				)),
			},
			verifyContents{
				name: "TestDiffDeleteDoesNotFollowParentSymlink",
				state: llb.Merge([]llb.State{
					busybox,
					linkFooToBar.File(llb.Mkfile("/bar/file", 0644, nil)),
					llb.Diff(createFooFile, createFooFile.File(llb.Rm("/foo/file"))),
				}),
				contents: contentsOf(runShell(busybox,
					"mkdir /bar",
					"touch /bar/file",
					"mkdir /foo",
				)),
			},

			verifyContents{
				name: "TestDiffCircularSymlinks",
				state: llb.Merge([]llb.State{
					busybox,
					llb.Diff(alpine, runShell(alpine, "ln -s /2 /1", "ln -s /1 /2")),
					llb.Scratch().
						File(llb.Mkfile("/1", 0644, []byte("foo"))),
				}),
				contents: contentsOf(runShell(busybox,
					"echo -n foo > /1",
					"ln -s /1 /2",
				)),
			},
			verifyContents{
				name: "TestDiffCircularDirSymlink",
				state: llb.Merge([]llb.State{
					busybox,
					llb.Diff(alpine, runShell(alpine, "mkdir /foo", "ln -s ../foo /foo/link")),
					llb.Diff(alpine, runShell(alpine, "mkdir -p /foo/link", "touch /foo/link/file")),
				}),
				contents: contentsOf(runShell(busybox,
					"mkdir -p /foo/link",
					"touch /foo/link/file",
				)),
			},
			verifyContents{
				name: "TestDiffCircularParentDirSymlinks",
				state: llb.Merge([]llb.State{
					busybox,
					llb.Diff(alpine, runShell(alpine, "ln -s /2 /1", "ln -s /1 /2")),
					llb.Diff(alpine, runShell(alpine, "mkdir /1", "echo -n foo > /1/file")),
				}),
				contents: contentsOf(runShell(busybox,
					"mkdir /1",
					"echo -n foo > /1/file",
					"ln -s /1 /2",
				)),
			},
		}
	}()...)

	// Test hardlinks
	tests = append(tests, func() []integration.Test {
		// TODO: note about how overlay+native differ inherently (disregarding diff+merge op), so we can't assert.

		linkedFiles := llb.Diff(alpine, runShell(alpine,
			"mkdir /dir",
			"touch /dir/1",
			"ln /dir/1 /dir/2",
			"chmod 0600 /dir/2",
		))
		chmodExecState := runShellExecState(busybox, "chmod 0777 /A/dir/1")
		chmodExecState.AddMount("/A", linkedFiles)
		mntB := chmodExecState.AddMount("/B", linkedFiles)
		return []integration.Test{
			verifyContents{
				name:  "TestDiffHardlinks",
				state: linkedFiles,
				contents: apply(
					fstest.CreateDir("/dir", 0755),
					fstest.CreateFile("/dir/1", nil, 0600),
					fstest.Link("/dir/1", "/dir/2"),
				),
			},
			verifyContents{
				name:  "TestDiffHardlinkChangesDoNotPropagateBetweenSnapshots",
				state: mntB,
				contents: apply(
					fstest.CreateDir("/dir", 0755),
					fstest.CreateFile("/dir/1", nil, 0600),
					fstest.Link("/dir/1", "/dir/2"),
				),
			},
		}
	}()...)

	// Diffs of lazy blobs should work.
	// TODO: add a test where you export a plain diff, not within a merge
	tests = append(tests,
		verifyContents{
			name: "TestDiffLazyBlobMerge",
			// Merge(A, Diff(A,B)) == B
			state:    llb.Merge([]llb.State{busybox, llb.Diff(busybox, alpine)}),
			contents: contentsOf(alpine),
		},
	)

	// Diffs of exec mounts should only include the changes made under the mount
	tests = append(tests, func() []integration.Test {
		splitDiffExecState := runShellExecState(alpine, "touch /root/A", "touch /mnt/B")
		splitDiffRoot := splitDiffExecState.Root()
		splitDiffMnt := splitDiffExecState.AddMount("/mnt", busybox)
		return []integration.Test{
			verifyContents{
				name:  "TestDiffExecRoot",
				state: llb.Diff(alpine, splitDiffRoot),
				contents: apply(
					fstest.CreateDir("/root", 0700),
					fstest.CreateFile("/root/A", nil, 0644),
				),
			},
			verifyContents{
				name:  "TestDiffExecMount",
				state: llb.Diff(busybox, splitDiffMnt),
				contents: apply(
					fstest.CreateFile("/B", nil, 0644),
				),
			},
		}
	}()...)

	// Diff+Merge combinations
	tests = append(tests, func() []integration.Test {
		a := llb.Scratch().File(llb.Mkfile("A", 0644, []byte("A")))
		b := llb.Scratch().File(llb.Mkfile("B", 0644, []byte("B")))
		c := llb.Scratch().File(llb.Mkfile("C", 0644, []byte("C")))
		deleteC := c.File(llb.Rm("C"))

		ab := llb.Merge([]llb.State{a, b})
		abc := llb.Merge([]llb.State{a, b, c})
		abDeleteC := llb.Merge([]llb.State{a, b, deleteC})

		nested := llb.Merge([]llb.State{
			abc.File(llb.Mkfile("D", 0644, []byte("D"))),
			llb.Merge([]llb.State{
				a,
				llb.Scratch().File(llb.Mkfile("E", 0644, []byte("E"))),
			}).File(llb.Rm("A")),
		})
		return []integration.Test{
			verifyContents{
				name: "TestDiffOnlyMerge",
				state: llb.Merge([]llb.State{
					llb.Diff(a, b),
					llb.Diff(b, a),
				}),
				contents: contentsOf(a),
			},

			// TODO: include this case when testing that diff blobs are reused when diff by one blob
			verifyContents{
				name:  "TestDiffOfMerges",
				state: llb.Diff(ab, abc),
				contents: apply(
					fstest.CreateFile("/C", []byte("C"), 0644),
				),
			},
			verifyContents{
				name:  "TestDiffOfMergesWithDeletes",
				state: llb.Merge([]llb.State{abc, llb.Diff(abc, abDeleteC)}),
				contents: apply(
					fstest.CreateFile("/A", []byte("A"), 0644),
					fstest.CreateFile("/B", []byte("B"), 0644),
				),
			},

			verifyContents{
				name:  "TestDiffSingleLayerOnMerge",
				state: llb.Diff(abDeleteC, abDeleteC.File(llb.Mkfile("D", 0644, []byte("D")))),
				contents: apply(
					fstest.CreateFile("/D", []byte("D"), 0644),
				),
			},
			verifyContents{
				name: "TestDiffSingleDeleteLayerOnMerge",
				state: llb.Merge([]llb.State{
					abDeleteC,
					llb.Diff(abc, abc.File(llb.Rm("A"))),
				}),
				contents: apply(
					fstest.CreateFile("/B", []byte("B"), 0644),
				),
			},
			verifyContents{
				name: "TestDiffMultipleLayerOnMerge",
				state: llb.Merge([]llb.State{
					abDeleteC,
					llb.Diff(abc, abc.
						File(llb.Mkfile("D", 0644, []byte("D"))).
						File(llb.Rm("A")),
					),
				}),
				contents: apply(
					fstest.CreateFile("/B", []byte("B"), 0644),
					fstest.CreateFile("/D", []byte("D"), 0644),
				),
			},

			verifyContents{
				name:  "TestDiffNestedLayeredMerges",
				state: llb.Diff(abc, nested.File(llb.Mkfile("F", 0644, []byte("F")))),
				contents: apply(
					fstest.CreateFile("/D", []byte("D"), 0644),
					fstest.CreateFile("/E", []byte("E"), 0644),
					fstest.CreateFile("/F", []byte("F"), 0644),
				),
			},
			verifyContents{
				name: "TestDiffNestedLayeredMergeDeletes",
				state: llb.Merge([]llb.State{
					ab.File(llb.Mkfile("D", 0644, []byte("D"))),
					llb.Diff(abc, nested.File(llb.Rm("D"))),
				}),
				contents: apply(
					fstest.CreateFile("/B", []byte("B"), 0644),
					fstest.CreateFile("/D", []byte("D"), 0644),
					fstest.CreateFile("/E", []byte("E"), 0644),
				),
			},
			// TODO: add a test case for merging deletes that don't apply (top level, sub-top level, files and dirs)

			// TODO: make sure there's coverage for when there's an intermediate delete that gets applied and then gets overwritten again

			// TODO: case where you do walking diff of overlay mounts (i.e. multi layer overlay diff) and the file that you link from is on
			// TODO: an intermediate layer whose parent dirs have metadata overriden by higher dirs which results in the dir not having a
			// TODO: overall metadata change (just an intermediate one)
		}
	}()...)

	// Diffs of diffs
	tests = append(tests, func() []integration.Test {
		a := llb.Scratch().File(llb.Mkfile("A", 0644, []byte("A")))
		ab := a.File(llb.Mkfile("B", 0644, []byte("B")))
		abc := ab.File(llb.Mkfile("C", 0644, []byte("C")))
		return []integration.Test{
			verifyContents{
				name: "TestDiffOfDiffs",
				state: llb.Diff(
					llb.Diff(a, ab),
					llb.Diff(a, abc),
				),
				contents: apply(
					fstest.CreateFile("/C", []byte("C"), 0644),
				),
			},
			verifyContents{
				name: "TestDiffOfDiffsWithDeletes",
				state: llb.Merge([]llb.State{
					abc,
					llb.Diff(
						llb.Diff(a, abc),
						llb.Diff(a, ab),
					),
				}),
				contents: apply(
					fstest.CreateFile("/A", []byte("A"), 0644),
					fstest.CreateFile("/B", []byte("B"), 0644),
				),
			},
		}
	}()...)

	// Diffs can be used as layer parents
	tests = append(tests, func() []integration.Test {
		a := llb.Scratch().File(llb.Mkfile("A", 0644, []byte("A")))
		ab := a.File(llb.Mkfile("B", 0644, []byte("B")))
		abc := ab.File(llb.Mkfile("C", 0644, []byte("C")))
		return []integration.Test{
			verifyContents{
				name:  "TestDiffAsParentSingleLayer",
				state: llb.Diff(a, ab).File(llb.Mkfile("D", 0644, []byte("D"))),
				contents: apply(
					fstest.CreateFile("B", []byte("B"), 0644),
					fstest.CreateFile("D", []byte("D"), 0644),
				),
			},
			verifyContents{
				name: "TestDiffAsParentSingleLayerDelete",
				state: llb.Merge([]llb.State{
					ab,
					llb.Diff(a, ab).File(llb.Rm("B")),
				}),
				contents: apply(
					fstest.CreateFile("A", []byte("A"), 0644),
				),
			},
			verifyContents{
				name:  "TestDiffAsParentMultipleLayers",
				state: llb.Diff(a, abc).File(llb.Mkfile("D", 0644, []byte("D"))),
				contents: apply(
					fstest.CreateFile("B", []byte("B"), 0644),
					fstest.CreateFile("C", []byte("C"), 0644),
					fstest.CreateFile("D", []byte("D"), 0644),
				),
			},
			verifyContents{
				name: "TestDiffAsParentMultipleLayerDelete",
				state: llb.Merge([]llb.State{
					ab,
					llb.Diff(a, abc).File(llb.Rm("B")),
				}),
				contents: apply(
					fstest.CreateFile("A", []byte("A"), 0644),
					fstest.CreateFile("C", []byte("C"), 0644),
				),
			},
		}
	}()...)

	return tests
}

/*
func testDiffOp(t *testing.T, sb integration.Sandbox) {
	type testCase struct {
		name     string
		state    llb.State
		contents contents
	}

	testCases := []testCase{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
		})
	}

	//
	//
	//
	//
	//

	requiresLinux(t)

	c, err := New(sb.Context(), sb.Address())
	require.NoError(t, err)
	defer c.Close()

	ctx := sb.Context()

	/* TODO:
	cdAddress := sb.ContainerdAddress()
	if cdAddress == "" {
		t.Skip("test requires containerd worker")
	}

	client, err := newContainerd(cdAddress)
	require.NoError(t, err)
	defer client.Close()

	ctdCtx := namespaces.WithNamespace(ctx, "buildkit")
	imageService := client.ImageService()
*/

/* TODO: cases
	* single layer slice diffs re-use blob input
	* diff where a single byte in a file changes but everything else is the same (hit the codepath that does byte comparison)
	* laziness

	// TODO: separate these into subtests and ensure they each use a clean cache
	// TODO: also add utils that lets you append generic assertions to each like that
	// they can be exported and then look the same when re-imported

	base := llb.Image("alpine:latest")
	base2 := llb.Image("busybox:latest")

	// Merges of diffs of lazy blobs should work.
	// This also tests that Merge(A, Diff(A,B)) == B
	requireEqualContents(ctx, t, c, llb.Merge([]llb.State{base, llb.Diff(base, base2)}), base2)

	// Diffs of identical states are empty
	requireContents(ctx, t, c, llb.Diff(llb.Scratch(), llb.Scratch()))
	requireContents(ctx, t, c, llb.Diff(base, base))
	requireEqualContents(ctx, t, c, llb.Merge([]llb.State{base, llb.Diff(base, base)}), base)

	// Diff of state with scratch has same contents as the state but without deletes
	requireEqualContents(ctx, t, c, llb.Diff(llb.Scratch(), base), base)

	requireContents(ctx, t, c, llb.Merge([]llb.State{
		llb.Scratch().
			File(llb.Mkfile("/foo", 0777, nil)),
		llb.Diff(llb.Scratch(), llb.Scratch().
			File(llb.Mkfile("/foo", 0777, nil)).
			File(llb.Rm("/foo")).
			File(llb.Mkfile("/bar", 0644, nil))),
	}), fstest.CreateFile("/foo", nil, 0777), fstest.CreateFile("/bar", nil, 0644))

	// Diff from a state to scratch is just a deletion of the contents of that state
	requireContents(ctx, t, c, llb.Merge([]llb.State{
		base,
		llb.Scratch().File(llb.Mkfile("/foo", 0644, []byte("foo"))),
		llb.Diff(base, llb.Scratch()),
	}), fstest.CreateFile("/foo", []byte("foo"), 0644))

	// Basic diff slices should work the same for single layer and multi-layer diffs
	diff := llb.Diff(base, runShell(base,
		"echo foo > /foo",
		"rm -rf /var",
	))
	requireContents(ctx, t, c, diff, fstest.CreateFile("/foo", []byte("foo\n"), 0644))
	requireEqualContents(ctx, t, c,
		llb.Merge([]llb.State{base2, diff}),
		base2.
			File(llb.Mkfile("/foo", 0644, []byte("foo\n"))).
			File(llb.Rm("/var")),
	)

	diff = llb.Diff(base, chainRunShells(base,
		[]string{"echo foo > /foo"},
		[]string{"rm -rf /var"},
	))
	requireContents(ctx, t, c, diff, fstest.CreateFile("/foo", []byte("foo\n"), 0644))
	requireEqualContents(ctx, t, c,
		llb.Merge([]llb.State{base2, diff}),
		base2.
			File(llb.Mkfile("/foo", 0644, []byte("foo\n"))).
			File(llb.Rm("/var")),
	)

	// Shuffling files back and forth should result in no diff (this requires special
	// handling in the overlay differ, which can otherwise be tricked into thinking
	// this is a diff because the file shows up in the upperdir)
	diff = llb.Diff(base, runShell(base, "mv /etc/motd /etc/foo", "mv /etc/foo /etc/motd"))
	requireContents(ctx, t, c, diff)
	requireEqualContents(ctx, t, c, llb.Merge([]llb.State{base, diff}), base)

	// TODO: explain this test case, making sure that /foo2/bar isn't deleted
	baseWithFoobar := runShell(base, "mkdir /foo", "touch /foo/bar")
	requireEqualContents(ctx, t, c,
		llb.Merge([]llb.State{
			runShell(base, "mkdir /foo2", "touch /foo2/bar"),
			llb.Diff(baseWithFoobar, runShell(baseWithFoobar, "rm -rf /foo", "ln -s /foo2 /foo")),
		}),
		runShell(base, "mkdir /foo2", "touch /foo2/bar", "ln -s /foo2 /foo"),
	)

	// TODO:
	// TODO:
	// TODO:
	// TODO:
	// TODO:
	// TODO:
	// TODO: this is a more important test case to verify dletes under a symlink aren't followed!
	baseWithFooMotd := runShell(base, "mkdir /foo", "touch /foo/motd")
	requireEqualContents(ctx, t, c,
		llb.Merge([]llb.State{
			runShell(base, "ln -s /etc /foo"),
			llb.Diff(baseWithFooMotd, runShell(baseWithFooMotd, "rm /foo/motd")),
		}),
		base,
	)

	// Applying to a base with a symlink should not result in following the symlink
	// during the apply

	// TODO: Also try an export+import cycle of this to make sure the archiver does the same thing

	// TODO: The fact that this did a diff of an exec on top of a blob broke something, ensure you have coverage for that

	// TODO: Add a note about the fact that this only works right now because adding /foo/baz results in /foo being
	// included in the exported tar for the layer. If that wasn't the case then if this was exported as a
	// an image and then reimported, the containerd applier would error out because it would try to create
	// /foo/baz, but see /foo is a symlink and return an error (in mkparents).
	// TODO: find a way to test that above, you need to make sure that applying a diff like that doesn't
	// result in a security vulnerability at min even if the behavior is weird.

	basePlusFoo := runShell(base, "mkdir -p /foo")
	merge := llb.Merge([]llb.State{
		// first create /foo as a symlink to /etc
		runShell(base, "ln -s /etc /foo"),
		// then apply a diff that adds /foo/bar, with /foo being a pre-existing dir, not a symlink
		llb.Diff(basePlusFoo, runShell(basePlusFoo, "touch /foo/bar")),
	})
	requireEqualContents(ctx, t, c,
		merge,
		base.
			File(llb.Mkdir("/foo", 0755)).
			File(llb.Mkfile("/foo/bar", 0644, nil)),
	)

	registry, err := sb.NewRegistry()
	if errors.Is(err, integration.ErrorRequirements) {
		t.Skip(err.Error())
	}
	require.NoError(t, err)

	def, err := merge.Marshal(sb.Context())
	require.NoError(t, err)
	imageTarget := registry + "/buildkit/testsymlinkdiffapply:latest"
	_, err = c.Solve(sb.Context(), def, SolveOpt{
		Exports: []ExportEntry{
			{
				Type: ExporterImage,
				Attrs: map[string]string{
					"name": imageTarget,
					"push": "true",
				},
			},
		},
	}, nil)
	require.NoError(t, err)

	/* TODO:
	img, err := imageService.Get(ctdCtx, imageTarget)
	require.NoError(t, err)

	err = imageService.Delete(ctdCtx, img.Name, images.SynchronousDelete())
	require.NoError(t, err)
	checkAllReleasable(t, c, sb, true)

	/*  TODO: debug check
	requireContents(ctx, t, c, llb.Merge([]llb.State{
		llb.Scratch().File(llb.Mkfile("/fjaklsjs", 0444, nil)),
		llb.Image(imageTarget),
	}))

	requireEqualContents(ctx, t, c,
		llb.Image(imageTarget),
		base.
			File(llb.Mkdir("/foo", 0755)).
			File(llb.Mkfile("/foo/bar", 0644, nil)),
	)

	// Intermediary changes that are undone should not appear in the diff across a chain
	requireEqualContents(ctx, t, c,
		llb.Merge([]llb.State{base2, llb.Diff(base, chainRunShells(base,
			[]string{"mv /var /foo", "touch /etc/bar", "touch /qaz"},
			[]string{"mv /foo /var", "rm /etc/bar", "touch /zaq"},
		))}),
		base2.
			File(llb.Mkfile("/qaz", 0644, nil)).
			File(llb.Mkfile("/zaq", 0644, nil)))

	//
	//
	//
	//
	//

	/* TODO:
	// TODO: document surprising behavior caused by /proc and /sys not existing in busybox base := llb.Image("busybox:latest")
	base := llb.Image("alpine:latest")
	diff := llb.Diff(base, base.
		Run(llb.Shlex("touch /yo")).Root().
		Run(llb.Shlex("mkdir /hello")).Root().
		Run(llb.Shlex("touch /hello/hey")).Root())

	def, err := diff.Marshal(sb.Context())
	require.NoError(t, err)

	destDir, err := ioutil.TempDir("", "buildkit")
	require.NoError(t, err)
	defer os.RemoveAll(destDir)

	_, err = c.Solve(sb.Context(), def, SolveOpt{
		Exports: []ExportEntry{
			{
				Type:      ExporterLocal,
				OutputDir: destDir,
			},
		},
	}, nil)
	require.NoError(t, err)

	require.NoError(t, fstest.CheckDirectoryEqualWithApplier(destDir, fstest.Apply(
		fstest.CreateFile("yo", nil, 0644),
		fstest.CreateDir("hello", 0755),
		fstest.CreateFile("hello/hey", nil, 0644),
	)))

	diff2 := llb.Diff(diff, diff.File(llb.Rm("/hello")))

	def, err = diff2.Marshal(sb.Context())
	require.NoError(t, err)

	destDir, err = ioutil.TempDir("", "buildkit")
	require.NoError(t, err)
	defer os.RemoveAll(destDir)

	_, err = c.Solve(sb.Context(), def, SolveOpt{
		Exports: []ExportEntry{
			{
				Type:      ExporterLocal,
				OutputDir: destDir,
			},
		},
	}, nil)
	require.NoError(t, err)

	require.NoError(t, fstest.CheckDirectoryEqualWithApplier(destDir, fstest.Apply()))

	merge := llb.Merge([]llb.State{diff, diff2}).File(llb.Mkfile("/sup", 0444, []byte("sup")))

	def, err = merge.Marshal(sb.Context())
	require.NoError(t, err)

	destDir, err = ioutil.TempDir("", "buildkit")
	require.NoError(t, err)
	defer os.RemoveAll(destDir)

	_, err = c.Solve(sb.Context(), def, SolveOpt{
		Exports: []ExportEntry{
			{
				Type:      ExporterLocal,
				OutputDir: destDir,
			},
		},
	}, nil)
	require.NoError(t, err)

	require.NoError(t, fstest.CheckDirectoryEqualWithApplier(destDir, fstest.Apply(
		fstest.CreateFile("yo", nil, 0644),
		fstest.CreateFile("sup", []byte("sup"), 0444),
	)))
}
*/

func testDiffOpExport(t *testing.T, sb integration.Sandbox) {
	requiresLinux(t)

	registry, err := sb.NewRegistry()
	if errors.Is(err, integration.ErrorRequirements) {
		t.Skip(err.Error())
	}
	require.NoError(t, err)

	c, err := New(sb.Context(), sb.Address())
	require.NoError(t, err)
	defer c.Close()

	base := llb.Image("alpine:latest")
	diff1 := llb.Diff(base, base.
		File(llb.Mkfile("1", 0444, []byte("1"))))
	diff2 := llb.Diff(base, base.
		File(llb.Mkdir("dir", 0755)).
		File(llb.Mkfile("dir/2", 0664, []byte("2"))))

	base2 := llb.Scratch().File(llb.Mkfile("base", 0777, []byte("base")))
	merge := llb.Merge([]llb.State{base2, diff1, diff2})

	def, err := merge.Marshal(sb.Context())
	require.NoError(t, err)

	imageTarget := registry + "/buildkit/testdiffexport:latest"
	_, err = c.Solve(sb.Context(), def, SolveOpt{
		Exports: []ExportEntry{
			{
				Type: ExporterImage,
				Attrs: map[string]string{
					"name": imageTarget,
					"push": "true",
				},
			},
		},
	}, nil)
	require.NoError(t, err)

	image := llb.Image(imageTarget)
	def, err = image.Marshal(sb.Context())
	require.NoError(t, err)

	destDir, err := ioutil.TempDir("", "buildkit")
	require.NoError(t, err)
	defer os.RemoveAll(destDir)

	_, err = c.Solve(sb.Context(), def, SolveOpt{
		Exports: []ExportEntry{
			{
				Type:      ExporterLocal,
				OutputDir: destDir,
			},
		},
	}, nil)
	require.NoError(t, err)

	require.NoError(t, fstest.CheckDirectoryEqualWithApplier(destDir, fstest.Apply(
		fstest.CreateFile("base", []byte("base"), 0777),
		fstest.CreateFile("1", []byte("1"), 0444),
		fstest.CreateDir("dir", 0755),
		fstest.CreateFile("dir/2", []byte("2"), 0664),
	)))
	// TODO: incorporate this case from Aaron into the rest of the test
	testMerge := llb.Merge([]llb.State{base, base2})
	testDiff := llb.Diff(testMerge, testMerge.File(llb.Mkfile("base3", 0777, nil)))
	def, err = testDiff.Marshal(sb.Context())
	require.NoError(t, err)
	_, err = c.Solve(sb.Context(), def, SolveOpt{
		Exports: []ExportEntry{
			{
				Type: ExporterImage,
				Attrs: map[string]string{
					"name": imageTarget,
					"push": "true",
				},
			},
		},
	}, nil)
	require.NoError(t, err)
}
