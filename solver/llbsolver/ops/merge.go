package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/worker"
	"github.com/pkg/errors"

	"github.com/moby/buildkit/cache"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/solver/llbsolver"
	"github.com/moby/buildkit/solver/pb"
	digest "github.com/opencontainers/go-digest"
)

const mergeCacheType = "buildkit.merge.v0"

type mergeOp struct {
	op     *pb.MergeOp
	worker worker.Worker
}

func NewMergeOp(v solver.Vertex, op *pb.Op_Merge, w worker.Worker) (solver.Op, error) {
	if err := llbsolver.ValidateOp(&pb.Op{Op: op}); err != nil {
		return nil, err
	}
	return &mergeOp{
		op:     op.Merge,
		worker: w,
	}, nil
}

func (m *mergeOp) CacheMap(ctx context.Context, group session.Group, index int) (*solver.CacheMap, bool, error) {
	dt, err := json.Marshal(struct {
		Type  string
		Merge *pb.MergeOp
	}{
		Type:  mergeCacheType,
		Merge: m.op,
	})
	if err != nil {
		return nil, false, err
	}

	cm := &solver.CacheMap{
		Digest: digest.Digest(dt),
		Deps: make([]struct {
			Selector          digest.Digest
			ComputeDigestFunc solver.ResultBasedCacheFunc
			PreprocessFunc    solver.PreprocessFunc
		}, len(m.op.Inputs)),
	}

	// TODO: ?
	// TODO: ?
	// TODO: ?
	/*
		for i := range cm.Deps {
			cm.Deps[i].ComputeDigestFunc = func(ctx context.Context, input solver.Result, s session.Group) (digest.Digest, error) {
				return digest.Digest(input.ID()), nil
			}
		}
	*/

	// TODO:
	// TODO:
	// TODO:
	// TODO:
	// TODO:
	bklog.G(ctx).Debugf("merge cache map: %+v", cm)

	return cm, true, nil
}

func (m *mergeOp) Exec(ctx context.Context, g session.Group, inputs []solver.Result) ([]solver.Result, error) {
	refs := make([]cache.ImmutableRef, len(inputs))
	ids := make([]string, len(inputs))
	// TODO:
	// TODO:
	// TODO:
	// TODO:
	descrs := make([]string, len(inputs))
	for i, inp := range inputs {
		wref, ok := inp.Sys().(*worker.WorkerRef)
		if !ok {
			return nil, errors.Errorf("invalid reference for merge %T", inp.Sys())
		}
		refs[i] = wref.ImmutableRef
		ids[i] = wref.ImmutableRef.ID()
		descrs[i] = strconv.Quote(wref.ImmutableRef.GetDescription())
	}

	// TODO:
	// TODO:
	// TODO:
	// TODO:
	// TODO:
	bklog.G(ctx).Debugf("merge exec inputs: %+v", inputs)
	bklog.G(ctx).Debugf("merge exec ids: %+v", ids)
	bklog.G(ctx).Debugf("merge exec descrs: %+v", descrs)

	mergedRef, err := m.worker.CacheManager().Merge(ctx, refs,
		cache.WithDescription(fmt.Sprintf("merge %s", strings.Join(ids, ";"))))
	if err != nil {
		return nil, err
	}

	return []solver.Result{worker.NewWorkerRefResult(mergedRef, m.worker)}, nil
}

func (m *mergeOp) Acquire(ctx context.Context) (release solver.ReleaseFunc, err error) {
	return func() {}, nil
}
