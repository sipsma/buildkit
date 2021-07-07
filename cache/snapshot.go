package cache

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/snapshot"
	"github.com/moby/buildkit/util/leaseutil"
	"github.com/moby/buildkit/util/winlayers"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

func (cr *cacheRecord) snapshotSize(ctx context.Context) (int64, error) {
	panic("TODO")
}

func (sr *immutableRef) releaseViewMount(ctx context.Context) error {
	panic("TODO")
}

func (sr *immutableRef) Mount(ctx context.Context, readonly bool, s session.Group) (snapshot.Mountable, error) {
	if err := sr.Extract(ctx, s); err != nil {
		return nil, err
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.cm.Snapshotter.Name() == "stargz" {
		var (
			m    snapshot.Mountable
			rerr error
		)
		if err := sr.withRemoteSnapshotLabelsStargzMode(ctx, s, func() {
			m, rerr = sr.mount(ctx)
		}); err != nil {
			return nil, err
		}
		return m, rerr
	}

	return sr.mount(ctx)
}

// must be called holding cacheRecord mu
func (sr *immutableRef) mount(ctx context.Context) (snapshot.Mountable, error) {
	if !sr.isFinalized {
		// if not finalized, there must be a mutable ref still around, return its mounts
		// but set read-only
		m, err := sr.cm.Snapshotter.Mounts(ctx, sr.getSnapshotID())
		if err != nil {
			return nil, errors.Wrapf(err, "failed to mount %s", sr.ID())
		}
		m = setReadonly(m)
		return m, nil
	}

	if sr.viewMount == nil {
		view := identity.NewID()
		l, err := sr.cm.LeaseManager.Create(ctx, func(l *leases.Lease) error {
			l.ID = view
			l.Labels = map[string]string{
				"containerd.io/gc.flat": time.Now().UTC().Format(time.RFC3339Nano),
			}
			return nil
		}, leaseutil.MakeTemporary)
		if err != nil {
			return nil, err
		}
		ctx = leases.WithLease(ctx, l.ID)
		m, err := sr.cm.Snapshotter.View(ctx, view, sr.getSnapshotID())
		if err != nil {
			sr.cm.LeaseManager.Delete(context.TODO(), leases.Lease{ID: l.ID})
			return nil, errors.Wrapf(err, "failed to mount %s", sr.ID())
		}
		sr.view = view
		sr.viewMount = m
	}
	return sr.viewMount, nil
}

func (sr *immutableRef) Extract(ctx context.Context, s session.Group) (rerr error) {
	if !sr.getBlobOnly() {
		return
	}

	ctx, done, err := leaseutil.WithLease(ctx, sr.cm.LeaseManager, leaseutil.MakeTemporary)
	if err != nil {
		return err
	}
	defer done(ctx)

	if sr.GetLayerType() == "windows" {
		ctx = winlayers.UseWindowsLayerMode(ctx)
	}

	if sr.cm.Snapshotter.Name() == "stargz" {
		if err := sr.withRemoteSnapshotLabelsStargzMode(ctx, s, func() {
			if rerr = sr.prepareRemoteSnapshotsStargzMode(ctx, s); rerr != nil {
				return
			}
			rerr = sr.extract(ctx, sr.descHandlers, s)
		}); err != nil {
			return err
		}
		return rerr
	}

	return sr.extract(ctx, sr.descHandlers, s)
}

func (sr *immutableRef) withRemoteSnapshotLabelsStargzMode(ctx context.Context, s session.Group, f func()) error {
	dhs := sr.descHandlers
	for _, r := range sr.parentRefChain() {
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

func (sr *immutableRef) prepareRemoteSnapshotsStargzMode(ctx context.Context, s session.Group) error {
	_, err := sr.sizeG.Do(ctx, sr.ID()+"-prepare-remote-snapshot", func(ctx context.Context) (_ interface{}, rerr error) {
		dhs := sr.descHandlers
		for _, r := range sr.parentRefChain() {
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
			if r.parent != nil {
				parentID = r.parent.getSnapshotID()
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

func (sr *immutableRef) extract(ctx context.Context, dhs DescHandlers, s session.Group) error {
	_, err := sr.sizeG.Do(ctx, sr.ID()+"-extract", func(ctx context.Context) (_ interface{}, rerr error) {
		snapshotID := sr.getSnapshotID()
		if _, err := sr.cm.Snapshotter.Stat(ctx, snapshotID); err == nil {
			return nil, nil
		}

		if sr.cm.Applier == nil {
			return nil, errors.New("extract requires an applier")
		}

		eg, egctx := errgroup.WithContext(ctx)

		parentID := ""
		if sr.parent != nil {
			eg.Go(func() error {
				if err := sr.parent.extract(egctx, dhs, s); err != nil {
					return err
				}
				parentID = sr.parent.getSnapshotID()
				return nil
			})
		}

		desc, err := sr.ociDesc()
		if err != nil {
			return nil, err
		}
		dh := dhs[desc.Digest]

		eg.Go(func() error {
			// unlazies if needed, otherwise a no-op
			return lazyRefProvider{
				ref:     sr,
				desc:    desc,
				dh:      dh,
				session: s,
			}.Unlazy(egctx)
		})

		if err := eg.Wait(); err != nil {
			return nil, err
		}

		if dh != nil && dh.Progress != nil {
			_, stopProgress := dh.Progress.Start(ctx)
			defer stopProgress(rerr)
			statusDone := dh.Progress.Status("extracting "+desc.Digest.String(), "extracting")
			defer statusDone()
		}

		key := fmt.Sprintf("extract-%s %s", identity.NewID(), sr.getChainID())

		err = sr.cm.Snapshotter.Prepare(ctx, key, parentID)
		if err != nil {
			return nil, err
		}

		mountable, err := sr.cm.Snapshotter.Mounts(ctx, key)
		if err != nil {
			return nil, err
		}
		mounts, unmount, err := mountable.Mount()
		if err != nil {
			return nil, err
		}
		_, err = sr.cm.Applier.Apply(ctx, desc, mounts)
		if err != nil {
			unmount()
			return nil, err
		}

		if err := unmount(); err != nil {
			return nil, err
		}
		if err := sr.cm.Snapshotter.Commit(ctx, sr.getSnapshotID(), key); err != nil {
			if !errors.Is(err, errdefs.ErrAlreadyExists) {
				return nil, err
			}
		}
		sr.queueBlobOnly(false)
		sr.queueSize(sizeUnknown)
		if err := sr.CommitMetadata(); err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

// caller must hold cacheRecord.mu
func (sr *immutableRef) finalize(ctx context.Context) error {
	if sr.isFinalized {
		return nil
	}
	if sr.mutableRef != nil {
		// can't commit the snapshot if someone still has an open mutable ref to it
		return errors.Wrap(ErrLocked, "cannot finalize record with open mutable ref")
	}

	if err := sr.cm.ManagerOpt.LeaseManager.AddResource(ctx, leases.Lease{ID: sr.ID()}, leases.Resource{
		ID:   sr.ID(),
		Type: "snapshots/" + sr.cm.ManagerOpt.Snapshotter.Name(),
	}); err != nil {
		sr.cm.LeaseManager.Delete(context.TODO(), leases.Lease{ID: sr.ID()})
		return errors.Wrapf(err, "failed to add snapshot %s to lease", sr.ID())
	}

	if err := sr.cm.Snapshotter.Commit(ctx, sr.ID(), sr.getSnapshotID()); err != nil {
		sr.cm.LeaseManager.Delete(context.TODO(), leases.Lease{ID: sr.ID()})
		return errors.Wrapf(err, "failed to commit %s", sr.getSnapshotID())
	}

	sr.isFinalized = true
	sr.queueSnapshotID(sr.ID())

	// If there is a hard-crash here, the old snapshot id written to metadata is no longer valid, but
	// this situation is checked for and fixed in cacheManager.getRecord.

	return sr.CommitMetadata()
}

func (sr *mutableRef) Mount(ctx context.Context, readonly bool, s session.Group) (snapshot.Mountable, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.cm.Snapshotter.Name() == "stargz" && sr.parent != nil {
		var (
			m    snapshot.Mountable
			rerr error
		)
		if err := sr.parent.withRemoteSnapshotLabelsStargzMode(ctx, s, func() {
			m, rerr = sr.mount(ctx, readonly)
		}); err != nil {
			return nil, err
		}
		return m, rerr
	}

	return sr.mount(ctx, readonly)
}

// must be called holding cacheRecord mu
func (sr *mutableRef) mount(ctx context.Context, readonly bool) (snapshot.Mountable, error) {
	m, err := sr.cm.Snapshotter.Mounts(ctx, sr.getSnapshotID())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to mount %s", sr.ID())
	}
	if readonly {
		m = setReadonly(m)
	}
	return m, nil
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
