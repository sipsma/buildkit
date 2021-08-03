package ops

import (
	"context"
	"encoding/json"

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

	for i := range m.op.Inputs {
		// TODO(sipsma) once there is fs metadata support these can be updated with funcs that only pull+operate on metadata
		cm.Deps[i].ComputeDigestFunc = llbsolver.NewContentHashFunc(nil)
		cm.Deps[i].PreprocessFunc = llbsolver.UnlazyResultFunc
	}

	return cm, true, nil
}

func (m *mergeOp) Exec(ctx context.Context, g session.Group, inputs []solver.Result) ([]solver.Result, error) {
	refs := make([]cache.ImmutableRef, len(inputs))
	for i, inp := range inputs {
		wref, ok := inp.Sys().(*worker.WorkerRef)
		if !ok {
			return nil, errors.Errorf("invalid reference for merge %T", inp.Sys())
		}
		refs[i] = wref.ImmutableRef
	}

	mergedRef, err := m.worker.CacheManager().Merge(ctx, refs)
	if err != nil {
		return nil, err
	}

	return []solver.Result{worker.NewWorkerRefResult(mergedRef, m.worker)}, nil
}

func (m *mergeOp) Acquire(ctx context.Context) (release solver.ReleaseFunc, err error) {
	return func() {}, nil
}
