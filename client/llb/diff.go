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
	// TODO: what happens server side when lower and/or upper is nil?
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
	proto.Platform = nil // merge op is not platform specific

	op := &pb.DiffOp{}

	op.Lower = &pb.LowerDiffInput{Input: pb.InputIndex(len(proto.Inputs))}
	pbLowerInput, err := m.lower.ToInput(ctx, constraints)
	if err != nil {
		return "", nil, nil, nil, err
	}
	proto.Inputs = append(proto.Inputs, pbLowerInput)

	op.Upper = &pb.UpperDiffInput{Input: pb.InputIndex(len(proto.Inputs))}
	pbUpperInput, err := m.upper.ToInput(ctx, constraints)
	if err != nil {
		return "", nil, nil, nil, err
	}
	proto.Inputs = append(proto.Inputs, pbUpperInput)

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

func (m *DiffOp) Inputs() []Output {
	return []Output{m.lower, m.upper}
}

func Diff(lower, upper State, opts ...ConstraintsOpt) State {
	var c Constraints
	for _, o := range opts {
		o.SetConstraintsOption(&c)
	}
	return NewState(NewDiff(lower, upper, c).Output())
}
