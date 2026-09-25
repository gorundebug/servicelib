package runtime

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestParallelCallbackPanicTerminatesProcess(t *testing.T) {
	const childEnv = "SERVICELIB_PARALLEL_PANIC_CHILD"
	const entered = "parallel-panic-callback-entered"
	const payload = "parallel-panic-original-payload"
	if os.Getenv(childEnv) != "" {
		app := &ServiceApp{}
		app.RunParallel(context.Background(), func() {
			fmt.Fprintln(os.Stderr, entered)
			panic(payload)
		})
		time.Sleep(5 * time.Second)
		t.Fatal("process survived an unhandled parallel callback panic")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestParallelCallbackPanicTerminatesProcess$")
	cmd.Env = append(os.Environ(), childEnv+"=1")
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("panic probe timed out: %s", output)
	}
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 2 {
		t.Fatalf("expected process exit 2, got %v: %s", err, output)
	}
	if !strings.Contains(string(output), entered) || !strings.Contains(string(output), payload) {
		t.Fatalf("missing callback entry or original panic: %s", output)
	}
}
