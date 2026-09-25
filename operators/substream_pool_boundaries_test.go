package operators

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/pool"
	"github.com/stretchr/testify/require"
)

type subStreamPoolConfig struct {
	subStreamTestConfig
	pools []*config.PoolConfig
}

func (c *subStreamPoolConfig) GetPools() []*config.PoolConfig { return c.pools }

func TestSubStreamPoolSlotAndDeferredResultBoundary(t *testing.T) {
	for kind := 0; kind < 3; kind++ {
		for _, samePool := range []bool{false, true} {
			t.Run(fmt.Sprintf("kind_%d_same_%t", kind, samePool), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				ctx = runtime.WithStreamId(ctx, "parent")
				poolConfig := &config.PoolConfig{Name: "worker", ExecutorsCount: 1}
				cfg := &subStreamPoolConfig{
					subStreamTestConfig: subStreamTestConfig{
						entry: config.SubStreamConfig{ID: 1, Name: "Entry", IdService: 1, IdSource: 2},
						body: config.MapStreamConfig{ID: 2, Name: "Result", IdService: 1, IdSource: 1},
					},
					pools: []*config.PoolConfig{poolConfig},
				}
				rc, err := config.NewRuntimeConfig(cfg)
				require.NoError(t, err)
				env := &subStreamTestEnv{}
				require.NoError(t, env.InitIsolatedGraphRuntime(rc, env, 1))
				entry, err := MakeSubStream[int, int](&cfg.entry, env)
				require.NoError(t, err)
				pending := make(chan func(), 1)
				body, err := MakeMapStream[int, int](&cfg.body, entry, MapHandler[int, int](func(callCtx context.Context, _ runtime.Stream, _ int, out runtime.Collect[int]) {
					pending <- func() { out.Out(callCtx, 42) }
				}))
				require.NoError(t, err)
				require.NoError(t, entry.SetSource(body))
				require.NoError(t, entry.Build())
				var scheduler pool.Pool
				var add func(context.Context, func()) error
				switch kind {
				case 0:
					p, err := pool.MakeTaskPool(env, poolConfig)
					require.NoError(t, err)
					scheduler, add = p, p.AddTask
				case 1:
					p, err := pool.MakePriorityTaskPool(env, poolConfig)
					require.NoError(t, err)
					scheduler = p
					add = func(ctx context.Context, fn func()) error { return p.AddTask(ctx, 0, fn) }
				default:
					p, err := pool.MakeDelayTaskPool(env)
					require.NoError(t, err)
					scheduler = p
					add = func(ctx context.Context, fn func()) error { return p.Delay(ctx, time.Millisecond, fn) }
				}
				require.NoError(t, scheduler.Start(context.Background()))
				defer scheduler.Stop(context.Background())
				independent, err := pool.MakeDelayTaskPool(env)
				require.NoError(t, err)
				require.NoError(t, independent.Start(context.Background()))
				defer independent.Stop(context.Background())
				var collected atomic.Int32
				var restored atomic.Bool
				completed := make(chan error, 1)
				require.NoError(t, add(ctx, func() {
					completed <- entry.Consume(ctx, 1, runtime.SubStreamCollectorFunc[int](func(returnCtx context.Context, value int) bool {
						restored.Store(returnCtx == ctx && value == 42)
						collected.Add(1)
						return true
					}))
				}))
				var emit func()
				select {
				case emit = <-pending:
				case <-ctx.Done():
					t.Fatal("SubStream did not enter its body")
				}
				started, finished := make(chan struct{}), make(chan struct{})
				respond := func() { close(started); emit(); close(finished) }
				if samePool {
					require.NoError(t, add(ctx, respond))
				} else {
					require.NoError(t, independent.Delay(ctx, time.Millisecond, respond))
				}
				occupiedSlot := samePool && kind != 2
				if occupiedSlot {
					select {
					case <-started:
						t.Error("waiting callback released its configured pool slot")
					case <-time.After(20 * time.Millisecond):
					}
					cancel()
				}
				select {
				case err := <-completed:
					if occupiedSlot { require.ErrorIs(t, err, context.Canceled) } else { require.NoError(t, err) }
				case <-time.After(5 * time.Second):
					t.Fatal("SubStream did not finish")
				}
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Fatal("accepted response callback was discarded")
				}
				if occupiedSlot {
					require.Zero(t, collected.Load())
				} else {
					require.Equal(t, int32(1), collected.Load())
					require.True(t, restored.Load())
				}
			})
		}
	}
}
