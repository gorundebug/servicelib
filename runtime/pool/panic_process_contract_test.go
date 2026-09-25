package pool

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const panicProbeEnvironment = "SERVICELIB_POOL_PANIC_CONTRACT_CHILD"
const panicProbeMessage = "pool-panic-contract-original-payload"

// Run the callback outside the test goroutine: an unrecovered worker panic
// must terminate the process, rather than merely fail the child test.
func TestPoolPanicProcessChild(t *testing.T) {
	kind := os.Getenv(panicProbeEnvironment)
	if kind == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	callback := func() {
		fmt.Println("pool-panic-contract-callback-entered")
		panic(panicProbeMessage)
	}
	switch kind {
	case "fifo":
		p := newTestTaskPool(t, "panic-contract", 1)
		require.NoError(t, p.Start(ctx))
		require.NoError(t, p.AddTask(ctx, callback))
	case "priority":
		p := newTestPriorityPool(t, "panic-contract", 1)
		require.NoError(t, p.Start(ctx))
		require.NoError(t, p.AddTask(ctx, 0, callback))
	case "delay":
		p := newTestDelayPool(t)
		require.NoError(t, p.Start(ctx))
		require.NoError(t, p.Delay(ctx, time.Millisecond, callback))
	default:
		t.Fatalf("unknown probe kind %q", kind)
	}
	<-ctx.Done()
	t.Fatal("pool-panic-contract-process-survived")
}

func TestPoolPanicTerminatesProcess(t *testing.T) {
	executable, err := os.Executable()
	require.NoError(t, err)
	for _, kind := range []string{"fifo", "priority", "delay"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, executable, "-test.run=^TestPoolPanicProcessChild$", "-test.count=1")
			command.Env = append(os.Environ(), panicProbeEnvironment+"="+kind)
			output, err := command.CombinedOutput()
			require.NoError(t, ctx.Err(), "panic probe hung: %s", output)
			var exited *exec.ExitError
			require.ErrorAs(t, err, &exited, "worker panic must terminate the process: %s", output)
			require.Equal(t, 2, exited.ExitCode(), "unexpected child failure: %s", output)
			require.Contains(t, string(output), "pool-panic-contract-callback-entered")
			require.Contains(t, string(output), "panic: "+panicProbeMessage)
			require.NotContains(t, string(output), "pool-panic-contract-process-survived")
			t.Logf("%s: process exited with code %d after callback panic", kind, exited.ExitCode())
		})
	}
}
