## TODO:(sipsma) update this with the rest of the docs once we're in agreement about the hardlink approach: https://github.com/moby/buildkit/pull/2212/files#diff-204a06e0f9a8baddea6b7213533cbf125d6a1194a9d9797988306229d26b05f1

# Summary
* Merges are performed by creating new empty snapshots and then applying merge inputs on top of one another, but with hardlinking of files instead of copying. If hardlinks aren't supported between files on the filesystem underlying the snapshotter, files will just be copied as usual.
* There will be an LLB MergeOp that accepts multiple input merge sources and options related to how conflicts between files are handled and whether deletions
  (i.e. whiteouts and opaques) are honored or not.
* Dockerfile's COPY statement can be optimized to use MergeOp instead of the existing llb.Copy FileOp provided no special features like wildcards or unpacking
  of input archives are required. In those cases, it will call MergeOp with arguments indicating that deletions should not be honored when merging in order to 
  replicate the behavior of a copy. Other users of MergeOp will want different behavior, such as wanting deletions to be honored.
  * Note that updating the Dockerfile frontend is ***not in scope for the current effort*** (just adding LLB support is), but we do obviously want to make sure the design here is compatible with potential future changes in that frontend.

# Example Case
```
FROM abc AS build1
...

FROM xyz AS build2
...

FROM ubuntu
COPY --from=build1 /out/foo /usr/bin/foo
COPY --from=build2 /out/bar /usr/bin/bar
```

***NOTE***: this example requires selector support, which is ***not in the current PR but can be added in the future***. For now, imagine that the `cache.Ref` interface is extended with something like a `.MergePaths(srcPath, destPath string)` method that allows you to specify dirs that should be sourced from and merged into when providing a ref to `cacheManager.Merge`.
* There are many other ways to support selectors during merges besides `MergePaths`; this is just a placeholder for these examples for now and not meant to be a final proposal.

### LLB
The set of `COPY` statements each get transformed into their own MergeOp. Because MergeOp is implemented lazily and `cacheManager` knows how to flatten repeated
merge refs into a single merge, the repeated `COPY` statements end up behaving the same as a merge with multiple inputs without any major slowdown from cache invalidation.

With that approach, the first copy, `COPY --from=build1 /out/foo /usr/bin/foo`, will look something like this in LLB:
```
{
	"Op": {
		"inputs": [{
			"digest": "sha256:ubuntusha",
			"index": 0
		}, {
			"digest": "sha256:build1sha",
			"index": 1
		}],
		"Op": {
			"merge": {
				"inputs": [{
					"input": 0
				}, {
					"input": 1
          "mergeSrc": "/out/foo",
          "mergeDest": "/usr/bin/foo"
				}]
			}
		},
		"constraints": {}
	},
	"Digest": "sha256:merge1sha",
	"OpMetadata": {
		"caps": null
	}
}
```
And the second something similar:
```
{
	"Op": {
		"inputs": [{
			"digest": "sha256:merge1sha",
			"index": 0
		}, {
			"digest": "sha256:build2sha",
			"index": 1
		}],
		"Op": {
			"merge": {
				"inputs": [{
					"input": 0
				}, {
					"input": 1
          "mergeSrc": "/out/bar",
          "mergeDest": "/usr/bin/bar"
				}]
			}
		},
		"constraints": {}
	},
	"Digest": "sha256:merge2sha",
	"OpMetadata": {
		"caps": null
	}
}
```

Alternatively, we could also have the Dockerfile parser identify repeated copy statements and turn them into a single MergeOp, i.e. something like this:
```
{
	"Op": {
		"inputs": [{
			"digest": "sha256:ubuntusha",
			"index": 0
		}, {
			"digest": "sha256:build1sha",
			"index": 1
		}, {
			"digest": "sha256:build2sha",
			"index": 1
		}],
		"Op": {
			"merge": {
				"inputs": [{
					"input": 0
				}, {
					"input": 1
          "mergeSrc": "/out/foo",
          "mergeDest": "/usr/bin/foo"
				}, {
					"input": 2
          "mergeSrc": "/out/bar",
          "mergeDest": "/usr/bin/bar"
				}]
			}
		},
		"constraints": {}
	},
	"Digest": "sha256:mergesha",
	"OpMetadata": {
		"caps": null
	}
}
```

But, as stated above, the difference should not be meaningful in terms of performance due to optimizations in `cacheManager`.

### Behavior
1. `FROM abc AS build1 ...`
   * immutableRef (w/ equalMutable) is created, call it `build1`
1. `FROM xyz AS build2 ...`
   * immutableRef (w/ equalMutable) is created, call it `build2`
1. `FROM ubuntu`
   * `GetByBlob` creates a lazy immutableRef, call it `ubuntu`
1. `COPY --from=build1 /out/foo /usr/bin/foo`
   * an immutableRef, call it `merge1`, is created by calling `cacheManager.Merge(ubuntu, build1.MergePaths("/out/foo", "/usr/bin/foo"))`
   * This results in the input refs (`ubuntu` and `build1`) to be finalized but not unlazied. No merged snapshot is created at this time. The `merge1` ref just holds references to its parents and will only create a merged snapshot on disk if mounts for it are requested.
1. `COPY --from=build2 /out/bar /usr/bin/bar`
   * an immutableRef, call it `merge2`, is created by calling `cacheManager.Merge(merge1, build2.MergePaths("/out/bar", "/usr/bin/bar"))`
   * `cacheManager.Merge` sees that one of its inputs is also a merge ref and "flattens" it out so that `merge2`'s parents are `ubuntu`, `build1.MergePaths(...)` and `build2.MergePaths(...)`.
1.  There is no `Run` on top of these merge refs, so nothing is ever unlazied. The final image will be exported as follows:
    1. `GetRemote` is called on `merge2`, which flattens it out into three sets of descriptors: 1 set for `ubuntu`'s layers (which stay lazy), 1 set for `build1` and 1 set for `build2`
    1. `ubuntu` has the same behavior as today; it's lazy so if it already exists in the destination being pushed to it's never pulled
    1. `build1` and `build2` will be turned into blobs the same way they are today with the exception that the blobs will be created by taking the contents of `/out/foo` from `build1` and putting them at `/usr/bin/foo` and similarly for `build2`.
       * ***Note that the way these blobs are created has nothing to do with MergeOp because we are not, in this case, merging `build1` or `build2`. This only has to do with hypothetical selector support***. Decisions on how these blobs are created can be made when selector support is added in the future but aren't in scope of merge support.
    1. That's it then. The merged ref is never turned into a blob itself because it doesn't need to be when there is no new ref created as child of it. `GetRemote` just traverses the refs of parents and only includes "layer refs" (not merged refs) in the chain of blobs, essentially linearizing them. Therefore, the merged
    snapshot is never created in this case.

# Extended Example Case
The last example didn't actually result in a merge snapshot being created on disk, so consider now this slight change:
```
FROM abc AS build1
...

FROM xyz AS build2
...

FROM ubuntu
COPY --from=build1 /out/foo /usr/bin/foo
COPY --from=build2 /out/bar /usr/bin/bar
RUN somecmd
```

### Behavior
Steps 1 through 5 (inclusive) are the same as above. Continuing from there:
1. `RUN somecmd`
   * A new mutable ref, call it `run1`, is created for the `RUN` from `merge2`, i.e. `run1 := cacheManager.New(merge2)`. `run1` is then mounted, which causes:
   * `ubuntu` to be unlazied (as would already happen today)
   * The snapshotter to need to actually create a merged snapshot on disk.
1. The specific way the merged snapshot is created depends on whether the underlying snapshotter uses overlay mounts or not and whether or not hardlinks are supported on the underlying filesystem of the snapshotter:
   * **Overlay snapshotters, hardlinks supported**
      1. A new, active snapshot is created with `ubuntu` as the parent snapshot and its mounts are obtained. The `upperdir` of this mount is extracted from the options. It will be an empty dir. Call this the `applyDir`.
      1. The mount for `build1` is obtained and the `lowerdir` option is extracted. The snapshotter then needs to apply the contents of `build1`'s subpath `/out/foo` to the above mentioned `applyDir` at `/usr/bin/foo`. In general, this will be done by hardlinking files found at+under `build1`'s `/out/foo` into the `applyDir` at `/usr/bin/foo` (directories have to be created though because they can't be hardlinked). The only issue is that you cannot hardlink from a mounted overlay to `applyDir`, so you have to find the `lowerdir` from `build1`'s mount that contains the file you want to hardlink in.
         * For the first iteration, we will deal with this in the simplest possible way. It will not be optimized but is still likely to be either faster than copies or not noticeably different in the vast majority of cases. See the below section `MergeOp vs. Copy Performance` for more discussion and possible optimizations.
         * This simple approach is to just traverse the lowerdirs **from top to bottom** and apply the contents of `/out/foo` to the `applyDir` at `/usr/bin/foo` (if there are any contents at `/out/foo`). If we encounter a file that was already hardlinked in from a higher layer, we skip it. 
      1. `build2` is then handled similarly to `build1`, applied to the same `applyDir` as above.
      1. The active snapshot that `applyDir` was extracted from is then committed. This is the merged snapshot.
   * **Bind-mount snapshotters (i.e. Native), hardlinks supported**
      1. A new, active snapshot is created with `ubuntu` as the parent snapshot and mounted. The `Source` of the bind mount is obtained and will be used as the `applyDir`, similar to above.
      1. The mount for `build1` is obtained. The snapshotter gets its `Source` directory and traverses starting at `/out/foo`, hardlinking files in and creating directories as needed under `applyDir` at `/usr/bin/foo`.
      1. The mount for `build2` is obtained and handled similarly as `build1`.
      1. The active snapshot that `applyDir` was extracted from is then committed. This is the merged snapshot.
   * **Other snapshotters and those without hardlink support**
      1. If there is an overlay or bind-mount based snapshotter on which hardlinks don't work (maybe the underlying storage FS doesn't support them) or a snapshotter that returns mounts other than overlay/bind, then a simple copy from the mounted input snapshots will be performed to create the merged snapshot. 
1. Once that merged snapshot is created, a new active one will be created on top of it, which will be used for the `RUN` statement.
1. Once the `RUN` finishes the image will need to be exported:
    1. `GetRemote` is called on `merge2`, which flattens it out into four sets of descriptors: 1 set for `ubuntu`'s layers, 1 set for `build1`, 1 set for `build2` and 1 single layer for `run1`.
    1. From here, the export is basically the same as the previous `Example Case` except that `run1` will also be turned into a blob and included with the descriptors in the `Remote`. Remember though that the layer for `run1` just consists of the layer created by running `somecmd`, ***it does not include the merged snapshot***. The merged snapshot is itself never turned into a blob. Instead, just the inputs to the merged snapshot are turned into blobs and included as part of the remote.

# MergeOp vs. Copy performance
The only possible case where performing a MergeOp instead of an llb.Copy could be theoretically slower is in the overlay case due to the fact that lowerdirs need to be iterated over to find where files are so they can be hardlinked in.

Given that 
1. a stat syscall typically has an execution time in the order of magnitude of 10s of microseconds
1. there can be at most 500 lowerdirs (kernel limit) but typical images rarely have much more than 10
1. the kernel's overlay implementation has to do something similar to search for lowerdirs anyways (though it saves some microseconds due to not needing to cross user->kernel space for each stat)

It's unlikely this could be a noticeable issue in all but the most extreme cases. It would require that the extra time spent performing stat calls outweighs the time saved from hardlinking instead of copying to a noticeable degree, which could only maybe happen if there's an enormous number of layers and lots of small files. In such extreme cases it seems plausible that MergeOp could take a few seconds longer than a copy. If we want to optimize these cases or find that the overhead is larger than expected, there are multiple ways we can optimize further:
  1. Just doing a copy from the mounted overlay if a file is below some size threshold instead of finding its lowerdir and hardlinking
  1. Caching which lowerdirs contain which files (fits well with the FSMetadata design from previous docs)
  1. Concurrently searching for lowerdirs (i.e. stat multiple lowerdirs in separate goroutines)

It's also worth noting that in those cases even if the execution time is a bit longer there would still be potential disk space savings, so it's still a tradeoff rather than a strict negative.

# MergeOp vs. Optimizing `llb.Copy`
The current proposal is to add new llb op called `MergeOp`. There is an alternative possibilty to instead modify the existing `llb.Copy` interface to perform the same optimizations as MergeOp when the right set of options are provided. This would at minimum require extending llb.Copy with options for whether deletions from layers should be included when copying (Dockerfile COPY doesn't want them but other use cases do).

* Pros of optimizing `llb.Copy`:
   1. Existing users of llb.Copy might benefit without as many changes as needing to switch to llb.Merge
* Cons:
   1. As mentioned above in the description of overlay based merges, obscure cases of merges could theoretically be slower than the plain copy. While it's still to be seen if the performance difference is meaningful, if it is we'd want to have a bool option for enabling/disabling whether `llb.Copy` can be implemented as Merge, which negates some of the benefit of the Pro above.
   1. The implementation of `llb.Copy` would become significantly more complicated (as opposed to extracting that complication out to a new op like MergeOp and keeping them separate)
  