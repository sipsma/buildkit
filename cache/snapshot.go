package cache

import (
	"context"
	"fmt"
	"strings"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/snapshot"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/leaseutil"
	"github.com/moby/buildkit/util/winlayers"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

func (sr *ImmutableRef) Mount(ctx context.Context, readonly bool, s session.Group) (snapshot.Mountable, error) {
	return sr.cacheRecord.Mount(ctx, true, sr.descHandlers, s)
}

func (sr *MutableRef) Mount(ctx context.Context, readonly bool, s session.Group) (snapshot.Mountable, error) {
	return sr.cacheRecord.Mount(ctx, readonly, sr.descHandlers, s)
}

func (cr *cacheRecord) Mount(ctx context.Context, readonly bool, dhs DescHandlers, s session.Group) (snapshot.Mountable, error) {
	if err := cr.PrepareMount(ctx, dhs, s); err != nil {
		return nil, err
	}
	mnt := cr.mountCache
	if readonly {
		mnt = setReadonly(mnt)
	}

	// TODO
	// TODO
	// TODO
	// TODO
	// TODO
	bklog.G(ctx).Debugf("ref %s mounts: %+v", cr.ID(), mnt)

	return mnt, nil
}

func (sr *ImmutableRef) PrepareMount(ctx context.Context, s session.Group) (rerr error) {
	return sr.cacheRecord.PrepareMount(ctx, sr.descHandlers, s)
}

func (sr *MutableRef) PrepareMount(ctx context.Context, s session.Group) (rerr error) {
	return sr.cacheRecord.PrepareMount(ctx, sr.descHandlers, s)
}

func (cr *cacheRecord) PrepareMount(ctx context.Context, dhs DescHandlers, s session.Group) (rerr error) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if cr.mountCache != nil {
		return nil
	}

	ctx, done, err := leaseutil.WithLease(ctx, cr.cm.LeaseManager, leaseutil.MakeTemporary)
	if err != nil {
		return err
	}
	defer done(ctx)

	if cr.GetLayerType() == "windows" {
		ctx = winlayers.UseWindowsLayerMode(ctx)
	}

	if cr.cm.Snapshotter.Name() == "stargz" {
		if err := cr.withRemoteSnapshotLabelsStargzMode(ctx, dhs, s, func() {
			if rerr = cr.prepareRemoteSnapshotsStargzMode(ctx, dhs, s); rerr != nil {
				return
			}
			rerr = cr.prepareMount(ctx, dhs, s)
		}); err != nil {
			return err
		}
		return rerr
	}

	return cr.prepareMount(ctx, dhs, s)
}

func (cr *cacheRecord) prepareMount(ctx context.Context, dhs DescHandlers, s session.Group) error {
	if cr.mountCache != nil {
		return nil
	}

	_, err := cr.sizeG.Do(ctx, cr.ID()+"-prepare-mount", func(ctx context.Context) (_ interface{}, rerr error) {
		eg, egctx := errgroup.WithContext(ctx)

		for _, rec := range cr.parentRecords() {
			eg.Go(func() error {
				return rec.prepareMount(egctx, dhs, s)
			})
		}

		var dh *DescHandler
		var desc ocispec.Descriptor
		if cr.getBlobOnly() {
			var err error
			desc, err = cr.ociDesc()
			if err != nil {
				return nil, err
			}
			dh = dhs[desc.Digest]

			eg.Go(func() error {
				// unlazies if needed, otherwise a no-op
				return lazyRefProvider{
					rec:     cr,
					desc:    desc,
					dh:      dh,
					session: s,
				}.Unlazy(egctx)
			})
		}

		if err := eg.Wait(); err != nil {
			return nil, err
		}

		// extract blob to snapshot if needed
		if cr.getBlobOnly() {
			if dh != nil && dh.Progress != nil {
				_, stopProgress := dh.Progress.Start(ctx)
				defer stopProgress(rerr)
				statusDone := dh.Progress.Status("extracting "+desc.Digest.String(), "extracting")
				defer statusDone()
			}

			var parentSnapshot string
			if cr.layerParent != nil {
				parentSnapshot = cr.layerParent.getSnapshotID()
			}
			if err := cr.cm.Snapshotter.Prepare(ctx, cr.getSnapshotID(), parentSnapshot); err != nil {
				return nil, err
			}

			mountable, err := cr.cm.Snapshotter.Mounts(ctx, cr.getSnapshotID())
			if err != nil {
				return nil, err
			}
			mounts, unmount, err := mountable.Mount()
			if err != nil {
				return nil, err
			}
			_, err = cr.cm.Applier.Apply(ctx, desc, mounts)
			if err != nil {
				unmount()
				return nil, err
			}

			if err := unmount(); err != nil {
				return nil, err
			}
			if err := cr.cm.Snapshotter.Commit(ctx, cr.getSnapshotID()); err != nil {
				if !errors.Is(err, errdefs.ErrAlreadyExists) {
					return nil, err
				}
			}
			cr.queueBlobOnly(false)
			cr.queueSize(sizeUnknown)
			if err := cr.commitMetadata(); err != nil {
				return nil, err
			}
		}

		// TODO How do you track progress when building merge+diff mounts inefficiently?

		mnt, err := cr.cm.Snapshotter.Mounts(leases.WithLease(ctx, cr.ID()), cr.getSnapshotID())
		if err != nil {
			return nil, errors.Wrapf(err, "failed to mount %s", cr.ID())
		}
		cr.mountCache = mnt

		return nil, nil
	})
	return err
}

func (cr *cacheRecord) commitLocked(ctx context.Context) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	return cr.commit(ctx)
}

// caller must hold cacheRecord.mu
func (cr *cacheRecord) commit(ctx context.Context) error {
	if cr.isCommitted {
		return nil
	}
	if cr.mutableRef != nil {
		// can't commit the snapshot if someone still has an open mutable ref to it
		return errors.Wrap(ErrLocked, "cannot commit record with open mutable ref")
	}

	// TODO need to recursively commit parents now because parents of a merge are allowed to not be committed?

	if err := cr.cm.Snapshotter.Commit(ctx, cr.getSnapshotID()); err != nil {
		return errors.Wrapf(err, "failed to commit %s", cr.getSnapshotID())
	}

	cr.isCommitted = true
	return nil
}

func (cr *cacheRecord) isLazy(ctx context.Context) (bool, error) {
	if !cr.getBlobOnly() {
		return false, nil
	}
	dgst := cr.getBlob()
	// special case for moby where there is no compressed blob (empty digest)
	if dgst == "" {
		return false, nil
	}
	_, err := cr.cm.ContentStore.Info(ctx, dgst)
	if errors.Is(err, errdefs.ErrNotFound) {
		return true, nil
	} else if err != nil {
		return false, err
	}

	// If the snapshot is a remote snapshot, this layer is lazy.
	// TODO fit in with snapshot.go?
	if info, err := cr.cm.Snapshotter.Stat(ctx, cr.getSnapshotID()); err == nil {
		if _, ok := info.Labels["containerd.io/snapshot/remote"]; ok {
			return true, nil
		}
	}

	return false, nil
}

func (cr *cacheRecord) size(ctx context.Context) (int64, error) {
	// this expects that usage() is implemented lazily
	s, err := cr.sizeG.Do(ctx, cr.ID(), func(ctx context.Context) (interface{}, error) {
		cr.mu.Lock()
		s := cr.getSize()
		if s != sizeUnknown {
			cr.mu.Unlock()
			return s, nil
		}
		// TODO use cr.snapshotSize
		driverID := cr.getSnapshotID()
		cr.mu.Unlock()
		var usage snapshots.Usage
		if !cr.getBlobOnly() {
			var err error
			usage, err = cr.cm.Snapshotter.Usage(ctx, driverID)
			if err != nil {
				cr.mu.Lock()
				isDead := cr.dead
				cr.mu.Unlock()
				if isDead {
					return int64(0), nil
				}
				if !errors.Is(err, errdefs.ErrNotFound) {
					return s, errors.Wrapf(err, "failed to get usage for %s", cr.ID())
				}
			}
		}
		if dgst := cr.getBlob(); dgst != "" {
			info, err := cr.cm.ContentStore.Info(ctx, digest.Digest(dgst))
			if err == nil {
				usage.Size += info.Size
			}
			for k, v := range info.Labels {
				// accumulate size of compression variant blobs
				if strings.HasPrefix(k, compressionVariantDigestLabelPrefix) {
					if cdgst, err := digest.Parse(v); err == nil {
						if digest.Digest(dgst) == cdgst {
							// do not double count if the label points to this content itself.
							continue
						}
						if info, err := cr.cm.ContentStore.Info(ctx, cdgst); err == nil {
							usage.Size += info.Size
						}
					}
				}
			}
		}
		cr.mu.Lock()
		cr.queueSize(usage.Size)
		if err := cr.commitMetadata(); err != nil {
			cr.mu.Unlock()
			return s, err
		}
		cr.mu.Unlock()
		return usage.Size, nil
	})
	if err != nil {
		return 0, err
	}
	return s.(int64), nil
}

func setReadonly(mounts snapshot.Mountable) snapshot.Mountable {
	return &readOnlyMounter{mounts}
}

type readOnlyMounter struct {
	snapshot.Mountable
}

func (m *readOnlyMounter) Mount() ([]mount.Mount, func() error, error) {
	mounts, release, err := m.Mountable.Mount()
	if err != nil {
		return nil, nil, err
	}
	for i, m := range mounts {
		if m.Type == "overlay" {
			mounts[i].Options = readonlyOverlay(m.Options)
			continue
		}
		opts := make([]string, 0, len(m.Options))
		for _, opt := range m.Options {
			if opt != "rw" {
				opts = append(opts, opt)
			}
		}
		opts = append(opts, "ro")
		mounts[i].Options = opts
	}
	return mounts, release, nil
}

func readonlyOverlay(opt []string) []string {
	out := make([]string, 0, len(opt))
	upper := ""
	for _, o := range opt {
		if strings.HasPrefix(o, "upperdir=") {
			upper = strings.TrimPrefix(o, "upperdir=")
		} else if !strings.HasPrefix(o, "workdir=") {
			out = append(out, o)
		}
	}
	if upper != "" {
		for i, o := range out {
			if strings.HasPrefix(o, "lowerdir=") {
				out[i] = "lowerdir=" + upper + ":" + strings.TrimPrefix(o, "lowerdir=")
			}
		}
	}
	return out
}

// TODO check this for merge ref
func (cr *cacheRecord) withRemoteSnapshotLabelsStargzMode(ctx context.Context, dhs DescHandlers, s session.Group, f func()) error {
	for _, r := range cr.layerChain() {
		r := r
		info, err := r.cm.Snapshotter.Stat(ctx, r.getSnapshotID())
		if err != nil && !errdefs.IsNotFound(err) {
			return err
		} else if errdefs.IsNotFound(err) {
			continue // This snpashot doesn't exist; skip
		} else if _, ok := info.Labels["containerd.io/snapshot/remote"]; !ok {
			continue // This isn't a remote snapshot; skip
		}
		desc, err := r.ociDesc()
		if err != nil {
			return err
		}
		dh := dhs[desc.Digest]
		if dh == nil {
			continue // no info passed; skip
		}

		// Append temporary labels (based on dh.SnapshotLabels) as hints for remote snapshots.
		// For avoiding collosion among calls, keys of these tmp labels contain an unique ID.
		flds, labels := makeTmpLabelsStargzMode(snapshots.FilterInheritedLabels(dh.SnapshotLabels), s)
		info.Labels = labels
		if _, err := r.cm.Snapshotter.Update(ctx, info, flds...); err != nil {
			return errors.Wrapf(err, "failed to add tmp remote labels for remote snapshot")
		}
		defer func() {
			for k := range info.Labels {
				info.Labels[k] = "" // Remove labels appended in this call
			}
			if _, err := r.cm.Snapshotter.Update(ctx, info, flds...); err != nil {
				logrus.Warn(errors.Wrapf(err, "failed to remove tmp remote labels"))
			}
		}()

		continue
	}

	f()

	return nil
}

func (cr *cacheRecord) prepareRemoteSnapshotsStargzMode(ctx context.Context, dhs DescHandlers, s session.Group) error {
	_, err := cr.sizeG.Do(ctx, cr.ID()+"-prepare-remote-snapshot", func(ctx context.Context) (_ interface{}, rerr error) {
		for _, r := range cr.layerChain() {
			r := r
			snapshotID := r.getSnapshotID()
			if _, err := r.cm.Snapshotter.Stat(ctx, snapshotID); err == nil {
				continue
			}

			desc, err := r.ociDesc()
			if err != nil {
				return nil, err
			}
			dh := dhs[desc.Digest]
			if dh == nil {
				// We cannot prepare remote snapshots without descHandler.
				return nil, nil
			}

			// tmpLabels contains dh.SnapshotLabels + session IDs. All keys contain
			// an unique ID for avoiding the collision among snapshotter API calls to
			// this snapshot. tmpLabels will be removed at the end of this function.
			defaultLabels := snapshots.FilterInheritedLabels(dh.SnapshotLabels)
			if defaultLabels == nil {
				defaultLabels = make(map[string]string)
			}
			tmpFields, tmpLabels := makeTmpLabelsStargzMode(defaultLabels, s)
			defaultLabels["containerd.io/snapshot.ref"] = snapshotID

			// Prepare remote snapshots
			var (
				key  = fmt.Sprintf("tmp-%s %s", identity.NewID(), r.getChainID())
				opts = []snapshots.Opt{
					snapshots.WithLabels(defaultLabels),
					snapshots.WithLabels(tmpLabels),
				}
			)
			parentID := ""
			if r.layerParent != nil {
				parentID = r.layerParent.getSnapshotID()
			}
			if err = r.cm.Snapshotter.Prepare(ctx, key, parentID, opts...); err != nil {
				if errdefs.IsAlreadyExists(err) {
					// Check if the targeting snapshot ID has been prepared as
					// a remote snapshot in the snapshotter.
					info, err := r.cm.Snapshotter.Stat(ctx, snapshotID)
					if err == nil { // usable as remote snapshot without unlazying.
						defer func() {
							// Remove tmp labels appended in this func
							for k := range tmpLabels {
								info.Labels[k] = ""
							}
							if _, err := r.cm.Snapshotter.Update(ctx, info, tmpFields...); err != nil {
								logrus.Warn(errors.Wrapf(err,
									"failed to remove tmp remote labels after prepare"))
							}
						}()

						// Try the next layer as well.
						continue
					}
				}
			}

			// This layer and all upper layers cannot be prepared without unlazying.
			break
		}

		return nil, nil
	})
	return err
}

func makeTmpLabelsStargzMode(labels map[string]string, s session.Group) (fields []string, res map[string]string) {
	res = make(map[string]string)
	// Append unique ID to labels for avoiding collision of labels among calls
	id := identity.NewID()
	for k, v := range labels {
		tmpKey := k + "." + id
		fields = append(fields, "labels."+tmpKey)
		res[tmpKey] = v
	}
	for i, sid := range session.AllSessionIDs(s) {
		sidKey := "containerd.io/snapshot/remote/stargz.session." + fmt.Sprintf("%d", i) + "." + id
		fields = append(fields, "labels."+sidKey)
		res[sidKey] = sid
	}
	return
}
