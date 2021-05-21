package llb

import (
	"context"
	"os"
	"testing"

	"github.com/moby/buildkit/solver/pb"
	"github.com/stretchr/testify/require"
)

func TestTmpfsMountError(t *testing.T) {
	t.Parallel()

	st := Image("foo").Run(Shlex("args")).AddMount("/tmp", Scratch(), Tmpfs())
	_, err := st.Marshal(context.TODO())

	require.Error(t, err)
	require.Contains(t, err.Error(), "can't be used as a parent")

	st = Image("foo").Run(Shlex("args"), AddMount("/tmp", Scratch(), Tmpfs())).Root()
	_, err = st.Marshal(context.TODO())
	require.NoError(t, err)

	st = Image("foo").Run(Shlex("args"), AddMount("/tmp", Image("bar"), Tmpfs())).Root()
	_, err = st.Marshal(context.TODO())
	require.Error(t, err)
	require.Contains(t, err.Error(), "must use scratch")
}

func TestValidGetMountIndex(t *testing.T) {
	// tests for https://github.com/moby/buildkit/issues/1520

	// tmpfs mount /c will sort later than target mount /b, /b will have output index==1
	st := Image("foo").Run(Shlex("args"), AddMount("/b", Scratch()), AddMount("/c", Scratch(), Tmpfs())).GetMount("/b")

	mountOutput, ok := st.Output().(*output)
	require.True(t, ok, "mount output is expected type")

	mountIndex, err := mountOutput.getIndex()
	require.NoError(t, err, "failed to getIndex")
	require.Equal(t, pb.OutputIndex(1), mountIndex, "unexpected mount index")

	// now swapping so the tmpfs mount /a will sort earlier than the target mount /b, /b should still have output index==1
	st = Image("foo").Run(Shlex("args"), AddMount("/b", Scratch()), AddMount("/a", Scratch(), Tmpfs())).GetMount("/b")

	mountOutput, ok = st.Output().(*output)
	require.True(t, ok, "mount output is expected type")

	mountIndex, err = mountOutput.getIndex()
	require.NoError(t, err, "failed to getIndex")
	require.Equal(t, pb.OutputIndex(1), mountIndex, "unexpected mount index")
}

func TestTODODeleteMe(t *testing.T) {
	img := Image("ubuntu")
	lcl := Local("foo")
	Git()
	Merge(img, lcl) // img<-lcl

	//
	//
	//

	a := Scratch().
		File(Mkfile("/foo", 0777, []byte("A"))).
		File(Mkfile("/a", 0777, []byte("A")))
	b := Scratch().
		File(Mkfile("/foo", 0777, []byte("B"))).
		File(Mkfile("/b", 0777, []byte("B")))

	Merge(a, b) /*
		/a   - contains A
		/b   - contains B
		/foo - contains B
	*/

	Merge(b, a) /*
		/a   - contains A
		/b   - contains B
		/foo - contains A
	*/

	//
	//
	//

	a := Scratch().
		File(Mkfile("/foo", 0777, []byte("A"))).
		File(Mkfile("/a", 0777, []byte("A")))
	b := a.
		File(Rm("/foo")).
		File(Mkfile("/b", 0777, []byte("B")))
	c := Scratch().
		File(Mkfile("/foo", 0777, []byte("C"))).
		File(Mkfile("/c", 0777, []byte("C")))

	Merge(b, c) /*
		/a   - contains A
		/b   - contains B
		/c   - contains C
		/foo - contains C
	*/

	Merge(c, b) /*
		/a   - contains A
		/b   - contains B
		/c   - contains C
		/foo - [doesn't exist!]
	*/

	//
	//
	//

	// TODO this doesn't really work well until we have DiffOp
	base := Image("ubuntu")
	serviceA := base.Run(Shlex("make A"))
	serviceB := base.Run(Shlex("make B"))
	Merge(serviceB, serviceA) // ubuntu<-"make B"<-ubuntu<-"make A"

	//
	//
	//

	base := Image("ubuntu")
	serviceA := base.Run(Shlex("make A")).Diff(base)
	serviceB := base.Run(Shlex("make B")).Diff(base)
	Merge(serviceB, serviceA) // "make B"<-"make A"

	//
	//
	//

	base := Image("ubuntu")
	serviceA := base.RunDiff(Shlex("make A"))
	serviceB := base.RunDiff(Shlex("make B"))
	Merge(serviceB, serviceA) // "make B"<-"make A"
}
