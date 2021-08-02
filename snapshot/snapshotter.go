package snapshot

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/docker/docker/pkg/idtools"
	"github.com/moby/buildkit/util/leaseutil"
	"github.com/pkg/errors"
)

type Mountable interface {
	Mount() ([]mount.Mount, func() error, error)
	IdentityMapping() *idtools.IdentityMapping
}

// Snapshotter defines interface that any snapshot implementation should satisfy
type Snapshotter interface {
	Name() string
	// Mounts returns the mounts for the given key. Unlike the containerd snapshotter interface, if the provided key is
	// in a committed state, read-only mounts will be returned for it (rather than forcing the caller to create a view
	// and get the mounts for that).
	Mounts(ctx context.Context, key string) (Mountable, error)
	Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) error
	Stat(ctx context.Context, key string) (snapshots.Info, error)
	Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error)
	Usage(ctx context.Context, key string) (snapshots.Usage, error)
	// Commit turns the snapshot (pointed to by the provided key) from active to committed. Unlike the containerd
	// snapshotter interface, this interface does not require you to change key.
	Commit(ctx context.Context, key string, opts ...snapshots.Opt) error
	Walk(ctx context.Context, fn snapshots.WalkFunc, filters ...string) error
	Close() error
	IdentityMapping() *idtools.IdentityMapping
	// Containerd returns the underlying containerd snapshotter interface used by this snapshotter
	Containerd() snapshots.Snapshotter
	AddLease(ctx context.Context, leaseID string, key string) error
	RemoveLease(ctx context.Context, leaseID string, key string) error
}

func FromContainerdSnapshotter(
	name string,
	s snapshots.Snapshotter,
	idmap *idtools.IdentityMapping,
	leaseManager leases.Manager,
) Snapshotter {
	return &fromContainerd{name: name, snapshotter: s, leaseManager: leaseManager, idmap: idmap}
}

type fromContainerd struct {
	mu           sync.Mutex
	name         string
	snapshotter  snapshots.Snapshotter
	leaseManager leases.Manager
	idmap        *idtools.IdentityMapping
}

func (sn *fromContainerd) activeKey(key string) string {
	if key == "" {
		return ""
	}
	split := strings.Split(key, "-")
	return split[0] + "-active"
}

func (sn *fromContainerd) commitKey(key string) string {
	if key == "" {
		return ""
	}
	split := strings.Split(key, "-")
	return split[0] + "-commit"
}

func (sn *fromContainerd) viewKey(key string) string {
	if key == "" {
		return ""
	}
	split := strings.Split(key, "-")
	return split[0] + "-view"
}

func (sn *fromContainerd) viewLeaseID(leaseID string) string {
	if leaseID == "" {
		return ""
	}
	split := strings.Split(leaseID, "-")
	return split[0] + "-view"
}

func (sn *fromContainerd) currentKey(ctx context.Context, key string) (string, error) {
	info, err := sn.Stat(ctx, key)
	if err != nil {
		return "", err
	}
	return info.Name, nil
}

func (sn *fromContainerd) Name() string {
	return sn.name
}

func (sn *fromContainerd) IdentityMapping() *idtools.IdentityMapping {
	return sn.idmap
}

func (sn *fromContainerd) Close() error {
	return sn.snapshotter.Close()
}

func (sn *fromContainerd) Containerd() snapshots.Snapshotter {
	return sn.snapshotter
}

func (sn *fromContainerd) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	// Figure out which snapshot ID is currently underlying the provided key, which
	// depends on whether the snapshot has been committed or is still active. Also check
	// "key" by itself in order to be compatible with buildkit installations that have
	// been upgraded from older versions that didn't use the same snapshot ID naming scheme.
	for _, key := range []string{sn.commitKey(key), sn.activeKey(key), key} {
		if info, err := sn.snapshotter.Stat(ctx, key); err == nil {
			return info, nil
		} else if !errdefs.IsNotFound(err) {
			return snapshots.Info{}, err
		}
	}

	return snapshots.Info{}, errors.Wrapf(errdefs.ErrNotFound, key)
}

func (sn *fromContainerd) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	key, err := sn.currentKey(ctx, key)
	if err != nil {
		return snapshots.Usage{}, err
	}
	return sn.snapshotter.Usage(ctx, key)
}

func (sn *fromContainerd) Mounts(ctx context.Context, key string) (_ Mountable, rerr error) {
	info, err := sn.Stat(ctx, key)
	if err != nil {
		return nil, err
	}
	key = info.Name

	if info.Kind == snapshots.KindCommitted {
		// setup a view for the mounts
		existingLease, ok := leases.FromContext(ctx)
		if !ok {
			return nil, fmt.Errorf("no lease to add %s view to", key)
		}
		leaseID := sn.viewLeaseID(existingLease)

		sn.mu.Lock()
		if _, err := sn.leaseManager.Create(ctx, func(l *leases.Lease) error {
			l.ID = leaseID
			l.Labels = map[string]string{
				"containerd.io/gc.flat": time.Now().UTC().Format(time.RFC3339Nano),
			}
			return nil
		}, leaseutil.MakeTemporary); err != nil && !errdefs.IsAlreadyExists(err) {
			sn.mu.Unlock()
			return nil, err
		} else if err == nil {
			defer func() {
				if rerr != nil {
					sn.leaseManager.Delete(context.TODO(), leases.Lease{ID: leaseID})
				}
			}()
		}
		sn.mu.Unlock()

		viewID := sn.viewKey(key)
		if _, err := sn.snapshotter.View(
			leases.WithLease(ctx, leaseID),
			viewID,
			key,
		); err != nil && !errdefs.IsAlreadyExists(err) {
			return nil, errors.Wrapf(err, "failed to create view for %s", key)
		}
		key = viewID
	}

	mounts, err := sn.snapshotter.Mounts(ctx, key)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get mounts")
	}
	return &staticMountable{mounts: mounts, idmap: sn.idmap, id: key}, nil
}

func (sn *fromContainerd) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) error {
	if parent != "" {
		curParent, err := sn.currentKey(ctx, parent)
		if err != nil {
			return err
		}
		parent = curParent
	}

	_, err := sn.snapshotter.Prepare(ctx, sn.activeKey(key), parent, opts...)
	return err
}

func (sn *fromContainerd) Commit(ctx context.Context, key string, opts ...snapshots.Opt) error {
	curKey, err := sn.currentKey(ctx, key)
	if err != nil {
		return err
	}
	return sn.snapshotter.Commit(ctx, sn.commitKey(key), curKey, opts...)
}

func (sn *fromContainerd) AddLease(ctx context.Context, leaseID string, key string) error {
	for _, id := range []string{sn.activeKey(key), sn.commitKey(key), key} {
		if err := sn.leaseManager.AddResource(ctx, leases.Lease{ID: leaseID}, leases.Resource{
			ID:   id,
			Type: "snapshots/" + sn.Name(),
		}); err != nil && !errdefs.IsAlreadyExists(err) {
			return err
		}
	}

	viewLeaseID := sn.viewLeaseID(leaseID)
	if _, err := sn.leaseManager.Create(ctx, func(l *leases.Lease) error {
		l.ID = viewLeaseID
		l.Labels = map[string]string{
			"containerd.io/gc.flat": time.Now().UTC().Format(time.RFC3339Nano),
		}
		return nil
	}, leaseutil.MakeTemporary); err != nil && !errdefs.IsAlreadyExists(err) {
		return err
	}
	if err := sn.leaseManager.AddResource(ctx, leases.Lease{ID: viewLeaseID}, leases.Resource{
		ID:   sn.viewKey(key),
		Type: "snapshots/" + sn.Name(),
	}); err != nil && !errdefs.IsAlreadyExists(err) {
		return err
	}

	return nil
}

func (sn *fromContainerd) RemoveLease(ctx context.Context, leaseID string, key string) error {
	for _, id := range []string{sn.activeKey(key), sn.commitKey(key), key} {
		if err := sn.leaseManager.DeleteResource(ctx, leases.Lease{ID: leaseID}, leases.Resource{
			ID:   id,
			Type: "snapshots/" + sn.Name(),
		}); err != nil && !errdefs.IsNotFound(err) {
			return err
		}
	}

	if err := sn.leaseManager.Delete(ctx, leases.Lease{ID: sn.viewLeaseID(leaseID)}); err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	return nil
}

func (sn *fromContainerd) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	info, err := sn.Stat(ctx, info.Name)
	if err != nil {
		return snapshots.Info{}, err
	}
	return sn.snapshotter.Update(ctx, info, fieldpaths...)
}

func (sn *fromContainerd) Walk(ctx context.Context, fn snapshots.WalkFunc, filters ...string) error {
	return sn.snapshotter.Walk(ctx, fn, filters...)
}
