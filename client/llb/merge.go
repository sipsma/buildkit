package llb

import (
	"context"

	"github.com/moby/buildkit/solver/pb"
	"github.com/opencontainers/go-digest"
)

type MergeOp struct {
	MarshalCache
	inputs []Output
	output Output
}

func NewMerge(inputs []State) *MergeOp {
	// TODO handle constraints? Worker constraints? Metadata? etc.
	op := &MergeOp{}
	for _, input := range inputs {
		op.inputs = append(op.inputs, input.Output())
	}
	op.output = &output{vertex: op}
	return op
}

func (m *MergeOp) Validate(ctx context.Context, constraints *Constraints) error {
	// TODO?
	return nil
}

func (m *MergeOp) Marshal(ctx context.Context, constraints *Constraints) (digest.Digest, []byte, *pb.OpMetadata, []*SourceLocation, error) {
	if m.Cached(constraints) {
		return m.Load()
	}
	if err := m.Validate(ctx, constraints); err != nil {
		return "", nil, nil, nil, err
	}

	proto, md := MarshalConstraints(constraints, &Constraints{})
	proto.Platform = nil // TODO right? Source seems to do this when things aren't platform specific

	op := &pb.MergeOp{}
	for _, input := range m.inputs {
		op.Inputs = append(op.Inputs, &pb.MergeInput{Input: pb.InputIndex(len(proto.Inputs))})
		pbInput, err := input.ToInput(ctx, constraints)
		if err != nil {
			return "", nil, nil, nil, err
		}
		proto.Inputs = append(proto.Inputs, pbInput)
	}
	proto.Op = &pb.Op_Merge{Merge: op}

	dt, err := proto.Marshal()
	if err != nil {
		return "", nil, nil, nil, err
	}

	// TODO m.Store(dt, md, m.constraints.SourceLocations, constraints)
	m.Store(dt, md, nil, constraints)
	return m.Load()
}

func (m *MergeOp) Output() Output {
	return m.output
}

func (m *MergeOp) Inputs() []Output {
	return m.inputs
}

func Merge(inputs []State) State {
	return NewState(NewMerge(inputs).Output())
}
