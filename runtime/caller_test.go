package runtime

import "testing"

func TestFunctionCallAsyncFlagOnlyChangesCallerMetadata(t *testing.T) {
	t.Parallel()

	if (&directCaller[int]{}).IsAsync() {
		t.Fatal("zero-value function-call caller must be synchronous")
	}
	if !(&directCaller[int]{async: true}).IsAsync() {
		t.Fatal("function-call caller must return its configured async flag")
	}
}
