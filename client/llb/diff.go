package llb

import (
	"context"

	"github.com/moby/buildkit/solver/pb"
	digest "github.com/opencontainers/go-digest"
)

type DiffOp struct {
	MarshalCache
	lower       Output
	upper       Output
	output      Output
	constraints Constraints
}

func NewDiff(lower, upper State, c Constraints) *DiffOp {
	op := &DiffOp{
		lower:       lower.Output(),
		upper:       upper.Output(),
		constraints: c,
	}
	op.output = &output{vertex: op}
	return op
}

func (m *DiffOp) Validate(ctx context.Context, constraints *Constraints) error {
	return nil
}

func (m *DiffOp) Marshal(ctx context.Context, constraints *Constraints) (digest.Digest, []byte, *pb.OpMetadata, []*SourceLocation, error) {
	if m.Cached(constraints) {
		return m.Load()
	}
	if err := m.Validate(ctx, constraints); err != nil {
		return "", nil, nil, nil, err
	}

	proto, md := MarshalConstraints(constraints, &m.constraints)
	proto.Platform = nil // diff op is not platform specific

	op := &pb.DiffOp{}

	op.Lower = &pb.LowerDiffInput{Input: pb.InputIndex(len(proto.Inputs))}
	if m.lower == nil {
		op.Lower.Input = pb.Empty
	} else {
		pbLowerInput, err := m.lower.ToInput(ctx, constraints)
		if err != nil {
			return "", nil, nil, nil, err
		}
		proto.Inputs = append(proto.Inputs, pbLowerInput)
	}

	op.Upper = &pb.UpperDiffInput{Input: pb.InputIndex(len(proto.Inputs))}
	if m.upper == nil {
		op.Upper.Input = pb.Empty
	} else {
		pbUpperInput, err := m.upper.ToInput(ctx, constraints)
		if err != nil {
			return "", nil, nil, nil, err
		}
		proto.Inputs = append(proto.Inputs, pbUpperInput)
	}

	proto.Op = &pb.Op_Diff{Diff: op}

	dt, err := proto.Marshal()
	if err != nil {
		return "", nil, nil, nil, err
	}

	m.Store(dt, md, m.constraints.SourceLocations, constraints)
	return m.Load()
}

func (m *DiffOp) Output() Output {
	return m.output
}

func (m *DiffOp) Inputs() (out []Output) {
	if m.lower != nil {
		out = append(out, m.lower)
	}
	if m.upper != nil {
		out = append(out, m.upper)
	}
	return out
}

func Diff(lower, upper State, opts ...ConstraintsOpt) State {
	if lower.Output() == nil && upper.Output() == nil {
		// diff of scratch and scratch is scratch
		return Scratch()
	}

	var c Constraints
	for _, o := range opts {
		o.SetConstraintsOption(&c)
	}
	return NewState(NewDiff(lower, upper, c).Output())
}

// TODO: In the future, consider adding a LayersBetween func that creates a merge of the diffs for each state betwen lower and upper

// TODO: add capability for diff