package client

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/continuity/fs/fstest"
	"github.com/moby/buildkit/client/llb"
	"github.com/moby/buildkit/util/testutil/integration"
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

	ctx := sb.Context()

	c, err := New(ctx, sb.Address())
	require.NoError(t, err)
	defer c.Close()

	requireContents(ctx, t, c, tc.state, tc.contents(sb))

	// export as an image, reimport and verify the image contents also match
	registry, err := sb.NewRegistry()
	if errors.Is(err, integration.ErrorRequirements) {
		t.Skip(err.Error())
	}
	require.NoError(t, err)

	def, err := tc.state.Marshal(sb.Context())
	require.NoError(t, err)
	imageTarget := fmt.Sprintf("%s/buildkit/%s:latest", registry, strings.ToLower(tc.name))
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

	// clear out all local state
	cdAddress := sb.ContainerdAddress()
	if cdAddress != "" {
		client, err := newContainerd(cdAddress)
		require.NoError(t, err)
		defer client.Close()
		ctdCtx := namespaces.WithNamespace(ctx, "buildkit")
		imageService := client.ImageService()
		img, err := imageService.Get(ctdCtx, imageTarget)
		require.NoError(t, err)
		err = imageService.Delete(ctdCtx, img.Name, images.SynchronousDelete())
		require.NoError(t, err)
	}
	checkAllReleasable(t, c, sb, true)

	requireContents(ctx, t, c, llb.Image(imageTarget), tc.contents(sb))
}

func diffOpTestCases() (tests []integration.Test) {
	alpine := llb.Image("alpine:latest")
	// busybox doesn't have /proc or /sys in the base image, add them
	// so they don't show up as diffs after execs.
	busybox := llb.Image("busybox:latest").
		File(llb.Mkdir("/proc", 0755)).
		File(llb.Mkdir("/sys", 0755))

	// Merges of diffs of lazy blobs should work.
	// This also tests that Merge(A, Diff(A,B)) == B
	tests = append(tests,
		verifyContents{
			name:     "TestDiffLazyBlobMerge",
			state:    llb.Merge([]llb.State{busybox, llb.Diff(busybox, alpine)}),
			contents: contentsOf(alpine),
		},
	)

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
					File(llb.Mkfile("/foo", 0777, nil)),
				llb.Diff(llb.Scratch(), llb.Scratch().
					File(llb.Mkfile("/foo", 0777, nil)).
					File(llb.Rm("/foo")).
					File(llb.Mkfile("/bar", 0644, nil))),
			}),
			contents: apply(
				fstest.CreateFile("/foo", nil, 0777),
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

	// Basic diff slices should work the same for single layer and multi-layer diffs
	tests = append(tests, func() []integration.Test {
		diffSingleLayer := llb.Diff(alpine, runShell(alpine,
			"echo foo > /foo",
			"rm -rf /var",
		))
		diffMultiLayer := llb.Diff(alpine, chainRunShells(alpine,
			[]string{"echo foo > /foo"},
			[]string{"rm -rf /var"},
		))

		diffContents := apply(fstest.CreateFile("/foo", []byte("foo\n"), 0644))
		rebaseContents := contentsOf(busybox.
			File(llb.Mkfile("/foo", 0644, []byte("foo\n"))).
			File(llb.Rm("/var")),
		)

		return []integration.Test{
			verifyContents{
				name:     "TestDiffSliceSingleLayer",
				state:    diffSingleLayer,
				contents: diffContents,
			},
			verifyContents{
				name:     "TestDiffSliceSingleLayerDeletes",
				state:    llb.Merge([]llb.State{busybox, diffSingleLayer}),
				contents: rebaseContents,
			},
			verifyContents{
				name:     "TestDiffSliceMultiLayer",
				state:    diffMultiLayer,
				contents: diffContents,
			},
			verifyContents{
				name:     "TestDiffSliceSingleLayerDeletes",
				state:    llb.Merge([]llb.State{busybox, diffMultiLayer}),
				contents: rebaseContents,
			},
		}
	}()...)

	// Shuffling files back and forth should result in no diff (this requires special
	// handling in the overlay differ, which can otherwise be tricked into thinking
	// this is a diff because the file shows up in the upperdir)
	tests = append(tests, func() []integration.Test {
		diff := llb.Diff(alpine, runShell(alpine,
			"mv /etc/motd /etc/foo",
			"mv /etc/foo /etc/motd",
		))
		return []integration.Test{
			verifyContents{
				name:     "TestDiffShuffle",
				state:    diff,
				contents: empty,
			},
			verifyContents{
				name:     "TestDiffShuffleDeletes",
				state:    llb.Merge([]llb.State{alpine, diff}),
				contents: contentsOf(alpine),
			},
		}
	}()...)

	// TODO: explain, symlink tests
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
					"mkdir /foo", // TODO: not sure, gotta test whether exporter makes this dir or symlink
				)),
			},

			// TODO: symlink to whiteout?
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
	* diffs of execs that have mounts
	* sliceable diff doesn't include intermediary deletes when merged
	* diff of "mv foo tmp && mv tmp foo" is empty
	* diffs where lower or upper or both are merges
	* diffs where deletes are in subdirectories
	* diffs where symlinks are overwritten
	* diffs where hardlinks have metadata changed
	* diff can be used as parent
	* diff can be mounted in exec
	* diff can be copied from
	* diff can be used as input to diff
	* merges of only diffs (right now you usually have a normal base in the merge)
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
