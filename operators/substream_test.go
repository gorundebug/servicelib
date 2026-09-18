package operators

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/serde"
	"github.com/stretchr/testify/require"
)

type subStreamTestConfig struct {
	config.Config
	entry config.SubStreamConfig
	body  config.MapStreamConfig
}

func (c *subStreamTestConfig) GetServices() []*config.ServiceConfig {
	return []*config.ServiceConfig{{ID: 1, Name: "test"}}
}
func (c *subStreamTestConfig) GetStreams() []config.StreamConfig {
	return []config.StreamConfig{&c.entry, &c.body}
}
func (*subStreamTestConfig) GetDataConnectors() []config.DataConnectorConfig { return nil }
func (*subStreamTestConfig) GetEndpoints() []config.EndpointConfig           { return nil }
func (*subStreamTestConfig) GetPools() []*config.PoolConfig                  { return nil }
func (*subStreamTestConfig) GetLinks() []*config.LinkConfig                  { return nil }
func (*subStreamTestConfig) GetModules() []*config.ModuleConfig              { return nil }
func (*subStreamTestConfig) GetTypes() []*config.TypeConfig                  { return nil }

type subStreamTestEnv struct{ runtime.ServiceApp }

func (*subStreamTestEnv) Metrics() metrics.Metrics { return (metrics.NoopMetricsEngine{}).Metrics() }
func (*subStreamTestEnv) Tracing() tracing.Tracing { return nil }
func (*subStreamTestEnv) GetSerde(reflect.Type) (serde.Serializer, error) {
	return nil, errors.New("test uses local values")
}

func newTestSubStream(t *testing.T, handler MapHandler[int, int]) runtime.TypedSubStream[int, int] {
	t.Helper()
	cfg := &subStreamTestConfig{
		entry: config.SubStreamConfig{ID: 1, Name: "Calculate", IdService: 1, IdSource: 2},
		body:  config.MapStreamConfig{ID: 2, Name: "Double", IdService: 1, IdSource: 1},
	}
	rc, err := config.NewRuntimeConfig(cfg)
	require.NoError(t, err)
	env := &subStreamTestEnv{}
	require.NoError(t, env.InitIsolatedGraphRuntime(rc, env, 1))
	entry, err := MakeSubStream[int, int](&cfg.entry, env)
	require.NoError(t, err)
	body, err := MakeMapStream[int, int](&cfg.body, entry, handler)
	require.NoError(t, err)
	require.NoError(t, entry.SetSource(body))
	require.NoError(t, entry.Build())
	return entry
}

func TestSubStreamCollectorCompletionDropsLateValues(t *testing.T) {
	entry := newTestSubStream(t, func(ctx context.Context, _ runtime.Stream, value int, out runtime.Collect[int]) {
		out.Out(ctx, value)
		out.Out(ctx, value+1)
		out.Out(ctx, value+2)
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var got []int
	err := entry.Consume(ctx, 10, runtime.SubStreamCollectorFunc[int](func(returnCtx context.Context, value int) bool {
		require.Equal(t, ctx, returnCtx)
		got = append(got, value)
		return len(got) == 2
	}))
	require.NoError(t, err)
	require.Equal(t, []int{10, 11}, got)
}

func TestSubStreamConcurrentCallsHaveIndependentCollectors(t *testing.T) {
	var workers sync.WaitGroup
	entry := newTestSubStream(t, func(ctx context.Context, _ runtime.Stream, value int, out runtime.Collect[int]) {
		workers.Add(1)
		go func() { defer workers.Done(); out.Out(ctx, value*2) }()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var calls sync.WaitGroup
	for value := 0; value < 100; value++ {
		calls.Add(1)
		go func() {
			defer calls.Done()
			got := -1
			err := entry.Consume(ctx, value, runtime.SubStreamCollectorFunc[int](func(_ context.Context, result int) bool {
				got = result
				return true
			}))
			if err != nil || got != value*2 {
				t.Errorf("value=%d result=%d err=%v", value, got, err)
			}
		}()
	}
	calls.Wait()
	workers.Wait()
}

func TestSubStreamCancellationDropsDelayedResult(t *testing.T) {
	var emit func()
	entry := newTestSubStream(t, func(ctx context.Context, _ runtime.Stream, value int, out runtime.Collect[int]) {
		emit = func() { out.Out(ctx, value) }
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	called := false
	err := entry.Consume(ctx, 1, runtime.SubStreamCollectorFunc[int](func(context.Context, int) bool {
		called = true
		return true
	}))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	emit()
	require.False(t, called)
	require.ErrorIs(t, entry.Consume(ctx, 1, nil), context.DeadlineExceeded)
	require.Error(t, entry.Consume(context.Background(), 1, nil))
}

func TestSubStreamNestedCallRestoresOuterInvocation(t *testing.T) {
	var entry runtime.TypedSubStream[int, int]
	entry = newTestSubStream(t, func(ctx context.Context, _ runtime.Stream, value int, out runtime.Collect[int]) {
		if value == 0 {
			out.Out(ctx, 1)
			return
		}
		err := entry.Consume(ctx, value-1, runtime.SubStreamCollectorFunc[int](func(returnCtx context.Context, result int) bool {
			out.Out(returnCtx, result+1)
			return true
		}))
		require.NoError(t, err)
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got := 0
	require.NoError(t, entry.Consume(ctx, 4, runtime.SubStreamCollectorFunc[int](func(returnCtx context.Context, result int) bool {
		require.Equal(t, ctx, returnCtx)
		got = result
		return true
	})))
	require.Equal(t, 5, got)
}

func TestSubStreamConcurrentResultsSerializeCollector(t *testing.T) {
	var workers sync.WaitGroup
	entry := newTestSubStream(t, func(ctx context.Context, _ runtime.Stream, count int, out runtime.Collect[int]) {
		for i := 0; i < count; i++ {
			workers.Add(1)
			go func() { defer workers.Done(); out.Out(ctx, 1) }()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	count := 0
	require.NoError(t, entry.Consume(ctx, 100, runtime.SubStreamCollectorFunc[int](func(context.Context, int) bool {
		count++
		return count == 50
	})))
	workers.Wait()
	require.Equal(t, 50, count)
}
