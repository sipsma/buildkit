package ops

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/moby/buildkit/worker"
	"github.com/pkg/errors"

	"github.com/moby/buildkit/cache"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/solver/llbsolver"
	"github.com/moby/buildkit/solver/pb"
	digest "github.com/opencontainers/go-digest"
)

const diffCacheType = "buildkit.diff.v0"

type diffOp struct {
	op     *pb.DiffOp
	worker worker.Worker
	vtx    solver.Vertex
}

func NewDiffOp(v solver.Vertex, op *pb.Op_Diff, w worker.Worker) (solver.Op, error) {
	if err := llbsolver.ValidateOp(&pb.Op{Op: op}); err != nil {
		return nil, err
	}
	return &diffOp{
		op:     op.Diff,
		worker: w,
		vtx:    v,
	}, nil
}

func (d *diffOp) CacheMap(ctx context.Context, group session.Group, index int) (*solver.CacheMap, bool, error) {
	dt, err := json.Marshal(struct {
		Type string
		Diff *pb.DiffOp
	}{
		Type: diffCacheType,
		Diff: d.op,
	})
	if err != nil {
		return nil, false, err
	}

	var depCount int
	if d.op.Lower.Input != pb.Empty {
		depCount++
	}
	if d.op.Upper.Input != pb.Empty {
		depCount++
	}

	cm := &solver.CacheMap{
		Digest: digest.Digest(dt),
		Deps: make([]struct {
			Selector          digest.Digest
			ComputeDigestFunc solver.ResultBasedCacheFunc
			PreprocessFunc    solver.PreprocessFunc
		}, depCount),
	}

	return cm, true, nil
}

func (d *diffOp) Exec(ctx context.Context, g session.Group, inputs []solver.Result) ([]solver.Result, error) {
	// TODO: handle when lower and/or upper are empty
	lowerEdge := d.vtx.Inputs()[d.op.Lower.Input]
	upperEdge := d.vtx.Inputs()[d.op.Upper.Input]
	var unmergeEdges []solver.Edge
	var isUnmerge bool
findUnmergePath:
	for {
		if upperEdge.Vertex.Digest() == lowerEdge.Vertex.Digest() && upperEdge.Index == lowerEdge.Index {
			isUnmerge = true
			break
		}
		unmergeEdges = append(unmergeEdges, upperEdge)
		if baseOp, ok := upperEdge.Vertex.Sys().(*pb.Op); ok {
			switch op := baseOp.Op.(type) {
			case *pb.Op_Exec:
				var mnt *pb.Mount
				for _, m := range op.Exec.Mounts {
					// TODO: not sure if this is correct
					if int(m.Output) == int(upperEdge.Index) {
						mnt = m
						break
					}
				}
				if mnt == nil {
					break findUnmergePath
				}
				upperEdge = upperEdge.Vertex.Inputs()[mnt.Input]
			case *pb.Op_File:
				var action *pb.FileAction
				for _, a := range op.File.Actions {
					if int(a.Output) == int(upperEdge.Index) {
						action = a
						break
					}
				}
				if action == nil {
					break findUnmergePath
				}
				upperEdge = upperEdge.Vertex.Inputs()[action.Input] // TODO: right? not secondary input?
			case *pb.Op_Merge:
			case *pb.Op_Source, *pb.Op_Merge, *pb.Op_Diff, *pb.Op_Build:
				// these ops are all bases in that they can't be layered on top of existing layers
				break findUnmergePath
			default:
				// TODO: should never be here, log it?
				break findUnmergePath
			}
		}
	}

	if isUnmerge {
		// create the result as a merging of diffs separating lower and upper
	}

	//
	//
	//
	//
	//

	var curInput int

	var lowerRef cache.ImmutableRef
	var lowerRefID string
	if d.op.Lower.Input != pb.Empty {
		if lowerInp := inputs[curInput]; lowerInp != nil {
			wref, ok := lowerInp.Sys().(*worker.WorkerRef)
			if !ok {
				return nil, errors.Errorf("invalid lower reference for diff op %T", lowerInp.Sys())
			}
			lowerRef = wref.ImmutableRef
			if lowerRef != nil {
				lowerRefID = wref.ImmutableRef.ID()
			}
		} else {
			return nil, errors.New("invalid nil lower input for diff op")
		}
		curInput++
	}

	var upperRef cache.ImmutableRef
	var upperRefID string
	if d.op.Upper.Input != pb.Empty {
		if upperInp := inputs[curInput]; upperInp != nil {
			wref, ok := upperInp.Sys().(*worker.WorkerRef)
			if !ok {
				return nil, errors.Errorf("invalid upper reference for diff op %T", upperInp.Sys())
			}
			upperRef = wref.ImmutableRef
			if upperRef != nil {
				upperRefID = wref.ImmutableRef.ID()
			}
		} else {
			return nil, errors.New("invalid nil upper input for diff op")
		}
	}

	if lowerRef == nil && upperRef == nil {
		// The diff of nothing and nothing is nothing. Just return an empty ref.
		return []solver.Result{worker.NewWorkerRefResult(nil, d.worker)}, nil
	}
	if lowerRef != nil && upperRef != nil && lowerRef.ID() == upperRef.ID() {
		// The diff of a ref and itself is nothing, return an empty ref.
		return []solver.Result{worker.NewWorkerRefResult(nil, d.worker)}, nil
	}

	diffRef, err := d.worker.CacheManager().Diff(ctx, lowerRef, upperRef,
		cache.WithDescription(fmt.Sprintf("diff %q -> %q", lowerRefID, upperRefID)))
	if err != nil {
		return nil, err
	}

	return []solver.Result{worker.NewWorkerRefResult(diffRef, d.worker)}, nil
}

func (d *diffOp) Acquire(ctx context.Context) (release solver.ReleaseFunc, err error) {
	return func() {}, nil
}
