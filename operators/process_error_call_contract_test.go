package operators

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/stretchr/testify/require"
)

type processErrorFixture struct {
	transformSerdeConfig
	links []*config.LinkConfig
}

func (c *processErrorFixture) GetLinks() []*config.LinkConfig { return c.links }

type processErrorEnvironment struct {
	subStreamTestEnv
	parallel sync.WaitGroup
}

func (e *processErrorEnvironment) RunParallel(_ context.Context, fn func()) {
	e.parallel.Add(1)
	go func() {
		defer e.parallel.Done()
		fn()
	}()
}

type processFailureValue struct { item int; identity *int }
type processFailureResult struct { ctx context.Context; value processFailureValue }

func TestProcessErrorLinkCallContract(t *testing.T) {
	for mode := 0; mode < 3; mode++ {
		for _, cancelled := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode_%d_cancel_%t", mode, cancelled), func(t *testing.T) {
				inputCfg := &config.InputStreamConfig{ID: 1, Name: "Root", IdService: 1}
				processCfg := &config.ProcessStreamConfig{ID: 2, Name: "Process", IdSource: 1, IdService: 1}
				successCfg := &config.MapStreamConfig{ID: 3, Name: "Success", IdSource: 2, IdService: 1}
				failureCfg := &config.MapStreamConfig{ID: 4, Name: "Failure", IdSource: -2, IdService: 1}
				semantics := &config.CallSemanticsGroup{FunctionCall: &config.FunctionCallSemanticsConfig{Async: mode == 1}}
				if mode == 2 { semantics = &config.CallSemanticsGroup{ParallelCall: &config.ParallelCallSemanticsConfig{}} }
				cfg := &processErrorFixture{
					transformSerdeConfig: transformSerdeConfig{streams: []config.StreamConfig{inputCfg, processCfg, successCfg, failureCfg}},
					links: []*config.LinkConfig{{From: -2, To: 4, CallSemantics: semantics}},
				}
				rc, err := config.NewRuntimeConfig(cfg)
				require.NoError(t, err)
				env := &processErrorEnvironment{}
				defer env.parallel.Wait()
				require.NoError(t, env.InitIsolatedGraphRuntime(rc, env, 1))
				input, err := MakeInputStream[int, any, any](inputCfg, env)
				require.NoError(t, err)
				identity := new(int)
				process, err := MakeProcessStream[int, int, processFailureValue](processCfg, input, ProcessHandler[int, int, processFailureValue](
					func(ctx context.Context, _ runtime.Stream, value int, out runtime.Collect[int], failure runtime.Collect[processFailureValue]) {
						failure.Out(ctx, processFailureValue{item: value, identity: identity})
						out.Out(ctx, value+1)
					}))
				require.NoError(t, err)
				require.Equal(t, -2, process.GetErrorStream().GetID())
				entered := make(chan struct{})
				// A separate release context allows idempotent early cleanup.
				releaseCtx, unblock := context.WithCancel(context.Background())
				defer unblock()
				failures := make(chan processFailureResult, 1)
				successes := make(chan int, 1)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				ctx = runtime.WithStreamId(ctx, "parent")
				_, err = MakeMapStream[processFailureValue, int](failureCfg, process.GetErrorStream(), MapHandler[processFailureValue, int](
					func(actual context.Context, _ runtime.Stream, value processFailureValue, _ runtime.Collect[int]) {
						close(entered)
						<-releaseCtx.Done()
						failures <- processFailureResult{actual, value}
					}))
				require.NoError(t, err)
				_, err = MakeMapStream[int, int](successCfg, process, MapHandler[int, int](func(actual context.Context, _ runtime.Stream, value int, _ runtime.Collect[int]) {
					require.Same(t, ctx, actual)
					successes <- value
				}))
				require.NoError(t, err)
				returned := make(chan struct{})
				go func() { input.Consume(ctx, 7); close(returned) }()
				select { case <-entered: case <-time.After(5*time.Second): t.Fatal("error handler did not start") }
				if mode == 2 {
					select { case <-returned: case <-time.After(5*time.Second): t.Fatal("parallel error link held caller") }
				} else {
					select { case <-returned: t.Fatal("direct error callback returned early"); case <-time.After(10*time.Millisecond): }
					require.Empty(t, successes)
				}
				if cancelled { cancel() }
				unblock()
				select { case <-returned: case <-time.After(5*time.Second): t.Fatal("caller did not return") }
				select {
				case failure := <-failures:
					require.Same(t, ctx, failure.ctx)
					require.Equal(t, cancelled, failure.ctx.Err() != nil)
					require.Equal(t, 7, failure.value.item)
					require.Same(t, identity, failure.value.identity)
				case <-time.After(5*time.Second): t.Fatal("accepted error handler did not complete")
				}
				require.Equal(t, 8, <-successes)
			})
		}
	}
}
