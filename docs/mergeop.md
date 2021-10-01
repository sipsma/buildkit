# Requirements
1. "Fast" merges that don't duplicate data on disk when using the right underlying snapshotter and have an execution time less than or equal to the previous llb.Copy implementation in vast majority of cases.
   * For copies specifically, `llb.Copy(src, dest)` must have the same complexity as `Merge(dest, llb.Copy(src, scratch))`, as described more below
1. Fallback merges that work with any snapshotter, also with execution time less than or equal to previous llb.Copy implementation
1. Merges are lazy in that they are not created as snapshots on disk until strictly needed.
1. When exporting an image created by merging in a separate llb.State, identical layers are not re-exported, they are re-used from their merged source.
   * If pushing to a remote registry that already has a layer input to a merge, no new layer is pushed
   * If a merge input doesn't exist locally (it's lazy) but does exist in the remote being pushed to, it stays lazy
1. Invalidation of one merge input does not result in other independent merge inputs being invalidated
   * Tonis's specific requirements around this are included as Examples below.
1. Support for optimizing Dockerfile `COPY` statements
1. Works with DiffOp

# Design
## `MergeOp`
1. MergeOp will be a new op that accepts multiple inputs and attempts to merge them in such a way as to minimize execution time and the amount of data duplicated on disk.
   * For the first iteration, merging will be implemented by creating hardlinks (as discussed more below). If hardlinks are not supported (perhaps due to a filesystem underlying the snapshotter not supporting them), then it will fallback to a less efficient copy.
   * We also leave open the option of implementing merges in other efficient ways in the future, such as the original solution of combining lowerdirs when using an overlay-based snapshotter (which worked but resulted in complicated code due to various quirks in overlay). Current tests show that hardlinks have very good performance while being relatively simple and universal across snapshotter types, so they are a good place to start.
1. The MergeOp API will only support merging the root (i.e. "/") of each input together. Support for selectors will be implemented by optimizing calls like `llb.Merge(A, llb.Scratch().File(llb.Copy(B, "/foo", "/usr/bar")))`, as described in a later section.

### Merged Snapshots
When `MergeOp` needs to create a merged ref, it will use a new cache API:
* `CacheManager.Merge(ctx context.Context, parents []ImmutableRef)`

This API will create a new type of lazy ref. Unlazying it results in the snapshot being created and only happens when a mount for the ref is requested. Merged snapshot creation works by repeating the following for each merge input:
1. Creating a new active snapshot on top of the current parent and obtaining its mounts. From those mounts, the directory onto which merged files+directories will be applied is obtained (call this the `applyDir`). How this is obtained depends on the mount type:
   * Overlay-based (i.e. overlay, stargz, fuse-overlayfs) - it will extract out the `upperdir`.
   * Bind  (i.e. native) - it will extract out the `Source` field.
   * Other (i.e. `btrfs` maybe) - it will just mount to a tmpdir and use that mount as the `applyDir`
1. Obtaining read-only mounts for the merge input ref and using those to apply the input contents to the `applyDir`. The exact way this works depends on the mount type:
   * Overlay-based:
      * Starting from the "uppermost" layer, the lowerdirs of each mount will be traversed top-to-bottom and applied to the `applyDir` by recreating directory objects and hardlinking in other file types.
      * If a file already exists in the `applyDir`, it will be skipped so that files from higher layers are preferred over those from lower layers.
      * Whiteout files will be applied by hardlinking in the whiteout device if nothing is present at the path already. Same goes for applying opaque xattrs on directories.
      * See the "Performance of Merge vs Copy" section in the appendix for more possible optimizations of this process
   * Bind-based:
      * Iterate over the mounts for each layer from top-to-bottom, using a differ to calculate the contents of each layer. Apply each diff to the `applyDir` by copying in directories, hardlinking in files and skipping files if they already exist in order to prefer the contents of higher layers over lower ones.
   * Other:
      * Same as bind-based, but just use copies instead of hardlinking.
1. Commit the active snapshot and return its key.

At this point the ref will have a snapshot ID whose contents represent the merge of the inputs.

### Merge Ref Blobs
Blobs for the merged ref will be created by just joining together the blobs of the input refs. Thus, if blobs for those input refs have already been calculated they can be re-used.

### Flattening Chained Merges
When multiple merges are chained together, we want to flatten them out so we just create one merge rather than a series of individual ones. Doing so is a significant optimization as it skips creating otherwise useless intermediaries.
* In other words, we want to turn `Merge(A, Merge(B, C))` into `Merge(A, B, C)`.

This works due to the fact that merge refs create lazy refs. No costly work is done at the time of ref creation, so `CacheManager.Merge` has the ability to identify and optimize chains before anything is unlazied. It will detect when an input ref to `Merge` is itself a merge and set its inputs to those "grandparent" inputs instead of the merge "parent".

## DiffOp
1. DiffOp will still be its own separate op as proposed before.
1. It will use a `CacheManager.Diff` API that creates a lazy ref:
   * `CacheManager.Diff(ctx context.Context, lower, upper ImmutableRef)`
1. Same basic idea as previous doc in terms of optimizing the overlay case by "slicing" lowerdirs and just running the differ otherwise. The case where the differ is run can use hardlinks now though, should be able to mostly re-use code.
   * This means that we will need to extend the `Snapshotter` API w/ a `Diff` method in order to handle the optimized overlay case. That's the only way we can keep the cache package agnostic of any overlay-specific details.
1. Turning a diff ref into blobs is straightforward too; when it's possible to "slice" refs to create the diff, blobs will just be re-used from the sliced inputs. Otherwise, a new blob will be created for for the diff ref, which will just be a single layer in this case.

## Dockerfile COPY Backend
In order to support the use of efficient merges as a backend for Dockerfile COPY statements, we need a way to integrate copies into merges (including import copy features like src and dest selectors, wildcards, etc.) without necessarily first performing the copy as a separate step before the merge. The approach here will be to make llb.Copy optionally lazy, in which case it can be efficiently incorporated into lazy merges.

### New `CacheManager.NewLazyRef` API
First, a new generic `CacheManager` API, `NewLazyRef`, will be added:
   * `CacheManager.NewLazyRef(ctx context.Context, parent ImmutableRef, retain []ImmutableRef, unlazy UnlazyFunc) (ImmutableRef, error)`

This API will be optionally used by FileOp for copy operations, allowing "copy refs" to be created that are lazy instead of immediately applying the copied contents to a new snapshsot as happens today.

The `parent` arg is set as the existing `cacheRecord.parent` field, which means that when `GetRemote` is called it will be the parent used in the generated descriptor chain.

`retain` can be specified in order to setup dependencies between this ref and other refs. Putting a ref in `retain` just enforces that those refs can't be removed while this child ref still exists and hasn't been unlazied. It's needed for llb.Copy to ensure that the input of the copy sticks around at least long enough for the copy operation to be performed. Once `unlazy` is called, the refs in `retain` will be released.

`unlazy` has type:
* `type UnlazyFunc func(ctx context.Context, mountable Mountable, retain []ImmutableRef) (fileChanges []FileChange, err error)`
* It will be called if the ref needs to be unlazied (i.e. in order to be mounted or in order for a child to be mounted).
* It's responsible for applying the contents of the ref to the provided mountable and returning an error if something goes wrong. E.g. this could can create a copy ref by mounting the provided mountable and copying in files from the snapshots of its input ref, which will be accessible via the `retain` arg.
* We want to provide a `Mountable` here (as opposed to an already mounted directory) because it allows certain optimizations, such as hardlinking directly to an underlying upperdir or bind mount source, to be implemented.
* It also returns the list of changes it made, which will be stored as part of the ref's metadata.
   * `FileChange` has type `type struct {ChangeKind: fs.ChangeKind, Path: string}`
   * It just indicates what kind of change was made at a given path, which allows the change to later be replicated without using differ algorithms, which are not conducive to reproducibility.
   * `cacheRecord`s will also be updated to support a new optional metadata field `equalLayerSnapshot` that, if set, indicates a separate snapshot ID that points to a layer with identical contents but different parent that the ref's main snapshot ID.
   * When `equalLayerSnapshot` and `fileChanges` have been set for a ref but the main snapshot ID has not been created yet, then blobs or the main snapshot ID can be calculated using that combination of `equalLayerSnapshot` + `fileChanges`, which will be more efficient than unpacking the ref from its original source.
* Once unlazy is called, it shouldn't be able to be called again and the refs in `retain` should be released.

### Lazy llb.Copy and Merge Optimizations
The next step will be to change `FileOp` so that copy actions are optionally implemented with `NewLazyRef`. This can be done by migrating the existing copy code to be run in the `unlazy` callback to `NewLazyRef`. We'll call these type of refs "copy refs".
* The `unlazy` callback can also include updates to use hardlinks instead of copies when possible. This would specifically be a possibility when no incompatible options are set, such as changing the owner or other file permissions. It can re-use the code used for hardlink merges here.

This feature will only be enabled if a new field in the LLB copy options is set to true: `PreferLazy bool`. If is is tricky to refactor existing FileOp code then we may also only enable this optimization in cases where copies are being made to a `Scratch` dest (as seen in examples below).

### Merge updates
Merge will need a straightforward update to be able to handle merge inputs that were created with `NewLazyRef`. When it encounters such an input ref, instead of unlazying the input ref and then merging it, it will call unlazy but with the `Mountable` for the merged snapshot.

For example, if Merge gets a lazy copy ref as an input, instead of first unlazying the copy into its own snapshot and then merging from that copy snapshot into the merge snapshot, it will just directly unlazy the copy to the merge snapshot.

It will additionally set `equalLayerSnapshot` on the unlazied ref, allowing the previously described optimizations around blob+snapshot creation to be enabled for it.

With all of the above, this should be able to serve as a backend for Dockerfile COPY. 

As an example:
```
FROM abc AS build1
...

FROM xyz AS build2
...

FROM ubuntu
COPY --from=build1 /out/foo /usr/bin/foo
COPY --from=build2 /out/bar /usr/bin/bar
RUN /usr/bin/foo && /usr/bin/bar
```
Will result in the following:
1. `COPY --from=build1 /out/foo /usr/bin/foo`
   * The client-side LLB will be `merge1 := llb.Merge([]llb.State{ubuntu, llb.Scratch().File(llb.Copy(build1, "/out/foo", "/usr/bin/foo"))})`
   * The copy step becomes a lazy copy ref:
      * `build1Copy := cacheManager.NewLazyRef(ctx, nil, []ImmutableRef{build1}, unlazyFunc(...))`
   * The merge step becomes a lazy merge ref:
      * `merge1 := cacheManager.Merge(ctx, []ImmutableRef{ubuntu, build1Copy})`
1. `COPY --from=build2 /out/bar /usr/bin/bar`
   * The client-side LLB will be `merge2 := llb.Merge([]llb.State{merge1, llb.Scratch().File(llb.Copy(build2, "/out/bar", "/usr/bin/bar"))})`
   * The copy step becomes a lazy copy ref:
      * `build2Copy := cacheManager.NewLazyRef(ctx, nil, []ImmutableRef{build2}, unlazyFunc(...))`
   * The merge step becomes a lazy merge ref:
      * `merge2 := cacheManager.Merge(ctx, []ImmutableRef{merge1, build2Copy})`
      * Internally, this will be flattened out to be `Merge(ctx, []ImmutableRef{ubuntu, build1Copy, build2Copy})`
1. `RUN /usr/bin/foo && /usr/bin/bar`
   * This will result in the mounting of a new ref whose parent is `merge2`, which will in turn result in `merge2` being unlazied.
   * When `merge2` is unlazied, it will start with the `ubuntu` snapshot as the base and then create a new active snapshot on top of it.
   * Then, `build1Copy` will be identified as a lazy ref, so its `unlazy` callback will be called with that new active snapshot mountable provided, resulting in `build1Copy`'s contents to be applied to it.
   * Then another new active snapshot will be created with the previous step as a parent and the process will repeat with `build2Copy`.
   * The final result is a snapshot chain that represents the merge of `ubuntu`, `build1Copy` and `build2Copy`
1. If this image is then exported:
   * The top layer will be the layer created by `RUN`
   * The next ref below that will be `merge2`, which `computeBlobChain` will see and recurse to each of its inputs in order `build2Copy`, `build1Copy` and `ubuntu`.
   * `build{1,2}Copy` will see that they have a layer created during the unlazying of `merge2` via the `equalLayerSnapshot` metadata, which means that their blobs can be computed directly from that layer in combination with the file changes as computed when their unlazy callbacks were called. `ubuntu` will have blobs computed as normal.

### Progress
Because copies will now be lazy, they will show up as DONE immediately. If they are later applied as part of a merge, then the time spent applying them will show up as progress on the merge.

# Examples
## `COPY --from` works by allowing rebases without pulling base image
```
FROM abc as build
...

FROM ubuntu
COPY --from=build /out/foo /usr/bin/foo
```
The above should never need to pull down ubuntu. What happens is:
1. `build` is created as normal, is an `immutableRef` w/ an `equalMutable`
1. `ubuntu` is created as normal, is a lazy `immutableRef`
1. `COPY --from=build /out/foo /usr/bin/foo`
   * This results in llb like `Merge(ubuntu, Scratch().File(Copy(build, "/out/foo", "/usr/bin/foo")))`
   * The copy step becomes a lazy copy ref:
      * `copyRef := cacheManager.NewLazyRef(ctx, nil, []ImmutableRef{build}, unlazyFunc(...))`
   * The merge step becomes a lazy merge ref:
      * `mergeRef := cacheManager.Merge(ctx, []ImmutableRef{ubuntu, copyRef})`
   * Note that because all refs have been lazy, no expensive work has happened and `ubuntu` remains lazy
1. On image export, the following will happen:
   1. `mergeRef` is the top ref in the chain. When it's reached in `computeBlobChain`, it will recurse to creating the blobs for its inputs.
      1. The input `copyRef` will not have any current blobs. Because `mergeRef` is also still lazy, there is no snapshot layer to be used to create the blob, so blobs for it will be created as usual, which requires unlazying the copy ref and then computing the blob from there.
      1. `ubuntu` is lazy but its blobs are already known because it was created by `GetByBlob`, so nothing is needed for it and it stays lazy
   1. The final `Remote` will have descriptors for `ubuntu` followed by those newly created for `copyRef`
   1. On push, provided that `ubuntu` already exists in the remote, it will never by unlazied.

## `COPY --from` pulls cache from dest layer without requiring cache for the origin stage
```
FROM abc AS build1
...

FROM xyz AS build2
...

FROM ubuntu
COPY --from=build1 /out/foo /usr/bin/foo
COPY --from=build2 /out/bar /usr/bin/bar
```
When the above is built with inline cache and then rebuilt with `--cache-from`, if the `build1` stage changed and `build2` didn't then `build2` should not be re-run and stay remote. What happens is:
1. On the first run:
   1. `build1` is created as normal, is an `immutableRef` w/ an `equalMutable`
   1. `build2` is created as normal, is an `immutableRef` w/ an `equalMutable`
   1. `ubuntu` is created as normal, is a lazy `immutableRef`
   1. `COPY --from=build1 /out/foo /usr/bin/foo`
      * This results in llb like `merge1 := Merge(ubuntu, Scratch().File(Copy(build1, "/out/foo", "/usr/bin/foo")))`
      * The copy step becomes a lazy copy ref:
         * `build1Copy := cacheManager.NewLazyRef(ctx, nil, []ImmutableRef{build1}, unlazyFunc(...))`
      * The merge step becomes a lazy merge ref:
         * `merge1Ref := cacheManager.Merge(ctx, []ImmutableRef{ubuntu, build1Copy})`
   1. `COPY --from=build2 /out/bar /usr/bin/bar`
      * This results in llb like `merge2 := Merge(merge1, Scratch().File(Copy(build2, "/out/bar", "/usr/bin/bar")))`
      * The copy step becomes a lazy copy ref:
         * `build2Copy := cacheManager.NewLazyRef(ctx, nil, []ImmutableRef{build2}, unlazyFunc(...))`
      * The merge step becomes a lazy merge ref:
         * `merge2Ref := cacheManager.Merge(ctx, []ImmutableRef{merge1, build2Copy})`
         * Internally, this will be flattened out to be more like `Merge(ctx, []ImmutableRef{ubuntu, build1Copy, build2Copy})`
   1. On image export, things will work essentially the same as the previous example but now the blobs will be created for both `build1Copy` and `build2Copy`. `ubuntu` is still just lazy though.
1. Now say that the previous image was exported with inline cache, all local cache is pruned and the same build is run again except with `--cache-from` and with a modification to the definition of `build1`:
   1. `build1` will need to be rebuilt locally due to a change invalidating the remote cache
   1. `build2` will not have changed so it won't need to be rebuilt locally.
   1. `ubuntu` will stay lazy as before
   1. `COPY --from=build1 /out/foo /usr/bin/foo`
      1. This vertex has inputs of `ubuntu` and `build1Copy`
      1. Because `build1` changed, `build1Copy` will be invalidated, which in turn invalidates this vertex, meaning it has to re-run.
      1. `MergeOp` will thus be executed with input refs for `ubuntu` and `build1Copy`, with `ubuntu` being a lazy blob and `build1Copy` a lazy copy ref (whose input `build1` exists locally).
   1. `COPY --from=build2 /out/bar /usr/bin/bar`
      1. This step has inputs of `merge1` and `build2Copy`
      1. Because one of those inputs, `merge1`, was invalidated, this step will be considered invalidated and have to re-run
      1. `MergeOp` will thus be executed with input refs for `merge1` and `build2Copy`, with `merge1` being a lazy merge ref from the previous step and `build2Copy` a lazy blob ref
         * `build2Copy` will be lazy because of the inline cache. The solver gets the key for the `build2Copy` vertex, searches the cache for it and gets a hit due to the remote cache, which included it as part of the exported image.
      1. `merge2` will be created as expected as `CacheManager.Merge(ctx, []ImmutableRef{ubuntu, build1Copy, build2Copy})` (after flattening), but remember that because it's created lazily, none of its parents or ancestors are unlazied and no expensive work takes place
   1. On image export, `merge2` is the top ref in the chain. `computeBlobChain` will thus recurse to each of its inputs:
      1. `build2Copy` will be a lazy blob ref, so nothing is needed
      1. `build1Copy` will not have any current blobs. Because `merge2` is also still lazy, there is no snapshot layer to be used to create the blob, so blobs for `build1Copy` will be created as usual, which requires unlazying the copy ref and then computing the blob from there.
      1. `ubuntu` will be a lazy blob ref, so nothing is needed
   1. Therefore, `ubuntu` and `build2Copy` stay lazy on this second run, with only `build1Copy` needing to be rebuilt and re-blobbed.

# Appendix
## Independent MergeOp vs. Optimized FileOp
Another possible approach was to modify the existing FileOp with new fields that enable efficient merging rather than add a new MergeOp.

One theoretical advantage to that approach would be that backwards compatibility is potentially easier as frontends won't need to ensure the buildkit daemon supports merge-op before sending LLB containing it.
   * However, there's a pretty big caveat to this. Namely, due to the way caching works, it's required that merges involving selectors be implemented as two separate vertexes (first one that projects the files and second one that does the merge).
   * That means the only way to model merges w/ selectors is to generate 2 llb.Copy calls that are embedded in each other. You couldn't have a single `FileOp` vertex that does both the selector and the merge.
   * The above approach would be backwards compatible in that it would still be valid LLB for older buildkit daemons. However, it would not be backwards compatible in that it would result in 2 copies on those old daemons, which would be much less efficient than the previous implementation.

Overall, the requirement that merges with selectors be implemented as 2 separate vertexes gets rid of the advantages of combining merge op into file op. While we could do it, we'd have to require that selectors and all other optional fields are not set when doing an efficient merge copy, at which point it feels like you're basically implementing two separate ops anyways.

Keeping merge separate also has some advantages in terms of keeping the internal code complexity a bit more separated and modular.

## Performance of Merge vs Copy
In the vast majority of cases, `Merge` should be faster than old `llb.Copy` calls in terms of snapshot creation. The only theoretical cases where a `Merge` could be noticeably slower are particularly extreme and would have to involve merges with inputs that have enormous numbers of layers (due to the fact that layers may need to be iterated over, depending on the mount type).

A few important points:
1. A stat syscall typically has an execution time in the order of magnitude of 10s of microseconds
1. There can be at most 500 lowerdirs (kernel limit) but typical images rarely have much more than 10
1. The kernel's overlay implementation has to do something similar to iterate over layers anyways when merging lowerdirs (though it saves some microseconds due to not needing to cross user->kernel space for each stat), so you pay some of this overhead when you use overlay rather than the underlying directories

Now consider the overlay snapshotter case:
* For the hardlink merge, you will need to iterate over each lowerdir of the inputs, stat'ing the files+dirs within, creating hardlinks for files that don't already exist (takes just 1 syscall) and recreating directory objects (takes a few syscalls to mkdir, chown, chmod, chtimes, etc.)
  * Compare this with the copy approach which has to do the same stat'ing and mkdir'ing plus extra overhead of performing a copy instead of a hardlink and the overhead introduced by overlayfs, but only has to do so for a single directory tree rather than N.
  * Overall, the worst case for the hardlink merge is an enormous number layers where only the top layer has the actual final contents (files from lowers just get overwritten) and all the files are small. In this case it will end up wasting time walking layers it doesn't need to and not save as much time from hardlinking vs copying due to the small size of the files.
* However, some more optimizations are possible to handle those worst cases better to the point where it's unlikely there will be any meaningful cases that are slower:
   1. A modified approach where you actually mount the merge input and walk that mount to identify files+dirs that should be merged into the `applyDir`. You can't hardlink in from the mount (as it would cross fs boundaries), so you still have to find the source of files in lowerdirs to perform the hardlink. However, you can optimize in a few extra ways now:
      * You can directly stat paths in lowerdirs rather than always fully walk all the dirs
      * You can stop searching once all files are found, as opposed to the previous solution that may end up iterating over lowerdirs even when there's no more new files that will end up in the `applyDir`
      * When you encounter a file below a certain size threshold, you can just copy it from the mount insted of spending time looking for the source.
      * You can more easily parallelize by searching for multiple files at the same time and multiple lowers at the same time
   1. Re-add the overlay specific optimization of joining lowerdirs, which works when the dest selector of an input is "/"
      * Note that this isn't a totally clean optimization on kernels pre-5.15 because it requires fixing unneeded opaque dirs, which adds one-time overhead to every snapshot creation (even those not currently part of a merge).

Another consideration is the blob creation step, where replacing copy with merge will allow for blobs to be re-used between builds more often. For example:
```
FROM abc as build1
...
FROM xyz as build2
...
FROM ubuntu
COPY --from=build1 /foo /bar
COPY --from=build2 /foo /bar
```
Say you already have run and exported this build previously so blobs are cached. If you change `build1` and re-export you will only have to create new blobs for `Copy(build1, ...)` if optimized merges are enabled, all other blobs can be re-used. This is better than the previous behavior where you would have to remake blobs for `Copy(build2, ...)` too, which can add up to a significant amount of time depending on the size of these inputs, often much more than just the time saved copying between snapshots.

Overall, based on early testing and the considerations above, it seems unlikely that merging will be slower than copying in all but the most extreme/obscure cases, especially once all possible optimizations are in place. It's also worth noting that in those cases even if the execution time is a bit longer there would still be potential disk space savings, so it's still a tradeoff rather than a strict negative.

## Previous Conflict Detection + FSMetadata Proposal
The previous set of proposals included support for serializing filesystem metadata and using that to implement conflict detection. While that can still be pursued in the long run, I think it makes sense to postpone it as it seems that the top priorities are (in no particular order):
* MergeOp
* DiffOp
* Optimizing Dockerfile COPY

None of those strictly require full blown conflict detection. This proposal instead just suggests that we implement the features needed for optimizing llb.Copy for now (whether deletions are included or not), which gives us the bare minimum from the previous proposal needed to get the above working (specifically the optimization of COPY).

Overall, while the previous proposal is worth consideration in the future as it could enable other optimizations, I don't think it all needs to be part of this current effort.

## Implementation Plan
This is the rough order of implementation that I think makes sense:
1. Implement MergeOp w/ support for hardlink merges
1. Implement DiffOp
1. Update FileOp to use `NewLazyRef` for copies and MergeOp to handle those lazy refs as inputs
1. Implement Dockerfile COPY support

Implementing the combination of Merge+Diff first is the shortest path to a basically useful featureset. This will allow us to validate the basic ideas without impacting any existing code like `FileOp`, where changes must be made much more carefully in order to retain backwards compatibility and integrate with existing code.

Once Merge+Diff are in place, we can more safely start integrating with `FileOp`. If any emergent changes needed to `Merge` crop up at this time, we can make them without being as concerned about retaining backwards compatibility because `MergeOp` and `DiffOp` will not yet be released (or can be released in only in an unstable "alpha" state).

# New Idea
1. Get rid of `NewLazyRef` and the idea of making copies lazy
1. Keep the idea of modelling `COPY --from=foo /a /b` as `Merge(base, Scratch.Copy(foo, /a, /b))`
1. Merge is still lazy, it uses hardlinks for the native snapshotter and the overlay optimized joining of lowerdirs for overlay-based snapshotters.
   * This includes the caveat of fixing up unneeded opaque dirs for the overlay case. That won't be an issue once kernels 5.15+ become commonplace, though that will take several years at least. We can think about ways of minimizing the downside here too once we're in agreement about the approach outlined here.
1. When merge creates a merged snapshot using hardlinks, it flattens all the inputs into a single snapshotter layer (whereas before it would create a new layer for each input).
   * This makes it especially important that it continue to flatten merge chains out (as in the previous proposal) so that `Merge(A, Merge(B, C))` becomes `Merge(A, B, C)`

This idea should result in improved performance over the existing Dockerfile COPY for all snapshotters. To see consider this example:
```
FROM ubuntu
COPY --from=foo /a /b
COPY --from=bar /c /d
RUN /blah
```

For the native snapshotter case (remember that the native snapshotter implements layers by copying all the contents of the parent to a child):
1. This is what happens today, without optimized merge:
   1. `COPY --from=foo /a /b`
      1. The snapshotter copies the contents of `ubuntu` to initialize the new layer
      1. `foo` gets copied as specified to that layer
      1. So the overall work is:
         * `copy(ubuntu, /, /) + copy(foo, /a, /b)`
   1. `COPY --from=bar /c /d`
      1. the snapshotter has to copy the previous snapshot copy to initialize the new layer
      1. `bar` gets copied as specified to that layer
      1. So the overall work is:
         * `copy(prevLayer1, /, /) + copy(bar, /c, /d)`
   1. `RUN /blah`
      1. the snapshotter has to copy the previous snapshot copy to initialize the new layer
      1. `/blah` gets ran
      1. So the overall work is:
         * `copy(prevLayer2, /, /) + run(/blah)`
   1. Summing up the total work done, including the copies inherent to the implementation of the native snapshotter:
      * `copy(ubuntu, /, /) + copy(foo, /a, /b) + copy(prevLayer1, /, /) + copy(bar, /c, /d) + copy(prevLayer2, /, /) + run(/blah)`
1. With the new idea, this is what will happen:
   1. `COPY --from=foo /a /b`
      1. This becomes `Merge(ubuntu, Scratch.Copy(foo, /a, /b))`.
      1. Merge is lazy, so no work happens there, but copy is done as previously, so it is done right away.
      1. So the overall work is:
         * `copy(foo, /a, /b)`
   1. `COPY --from=bar /c /d`
      1. This becomes `Merge(prevLayer1, Scratch.Copy(bar, /c, /d))`.
      1. Merge is lazy, so no work happens there, but copy is done as previously, so it is done right away.
      1. So the overall work is:
         * `copy(bar, /c, /d)`
   1. `RUN /blah`
      1. The merge needs to be unlazied now, which requires creating a new empty snapshot and then hardlinking all the inputs in.
      1. So the overall work is:
         * `hardlink(ubuntu, /, /) + hardlink(foo, /a, /b) + hardlink(bar, /c, /d) + run(/blah)`
   1. Summing up the total work done:
      * `copy(foo, /a, /b) + copy(bar, /c, /d) + hardlink(ubuntu, /, /) + hardlink(foo, /a, /b) + hardlink(bar, /c, /d) + run(/blah)`
1. Subtracting the amount of work required required for the new idea from the amount of work required today you get:
   * `copy(ubuntu, /, /)`
   * `+ copy(prevLayer1, /, /)`
   * `+ copy(prevLayer2, /, /)`
   * `- hardlink(ubuntu, /, /)`
   * `- hardlink(foo, /a, /b)` 
   * `- hardlink(bar, /c, /d)`
   * Hardlinking in the native snapshotter case should never be slower than copying in these cases (and will very often be significantly faster), so it should be clear that the new approach is more performant than the previous one.

For the overlay snapshotter case:
1. This is what happens today:
   1. `COPY --from=foo /a /b`
      1. The snapshotter creates a new layer on top of `ubuntu` and copies foo into that as specified
      1. So the overall work is:
         * `copy(foo, /a, /b)`
   1. `COPY --from=bar /c /d`
      1. The snapshotter creates a new layer on top of the previous and copies bar into that as specified
      1. So the overall work is:
         * `copy(bar, /c, /d)`
   1. `RUN /blah`
      1. the snapshotter creates a new empty layer on top of the previous
      1. So the overall work is:
         * `run(/blah)`
   1. Summing up the total work done:
      * `copy(foo, /a, /b) + copy(bar, /c, /d) + run(/blah)`
1. This is what happens today:
   1. `COPY --from=foo /a /b`
      1. This becomes `Merge(ubuntu, Scratch.Copy(foo, /a, /b))`.
      1. Merge is lazy, so no work happens there, but copy is done as previously, so it is done right away.
      1. So the overall work is:
         * `copy(foo, /a, /b)`
   1. `COPY --from=bar /c /d`
      1. This becomes `Merge(prevLayer1, Scratch.Copy(bar, /c, /d))`.
      1. Merge is lazy, so no work happens there, but copy is done as previously, so it is done right away.
      1. So the overall work is:
         * `copy(bar, /c, /d)`
   1. `RUN /blah`
      1. The merge has to be unlazied, but is implemented by just joining together lowerdirs, which is all in memory and thus can be rounded to 0 for our purposes here.
      1. So the overall work is just:
         * `run(/blah)`
   1. Summing up the total work done:
      * `copy(foo, /a, /b) + copy(bar, /c, /d) + run(/blah)`
1. Overall, the amount of work done between these two examples is the same. However, there are still benefits in this case in terms of caching:
   1. The examples from previous sections still apply ("`COPY --from` works by allowing rebases without pulling base image" and "`COPY --from` pulls cache from dest layer without requiring cache for the origin stage")
   1. Additionally, because copies are now made on top of scratch and then merged to a different parent independently, if you later did a build with a different set of merge inputs but the same copy input, no redundant work would be needed for all those other merges.
1. Note that the issue around unnecessary opaque dirs should not actually be an issue here at all because each COPY is made to scratch, which means that its a single layer snapshot, which in turn means it will just be a bind mount and thus not have unneeded opaques. The opaque issue only matters to cases where you have non-base merge inputs that are multi-layered.
