package operators

import (
	"context"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/dataquery"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/stretchr/testify/require"
)

type queryBoundaryConfig struct {
	queryConfigFixture
	links []*config.LinkConfig
}

func (c *queryBoundaryConfig) GetLinks() []*config.LinkConfig { return c.links }

type queryBoundaryHandler struct{ ended chan struct{} }

func (*queryBoundaryHandler) BeginRequest(ctx context.Context, _ runtime.Stream) (context.Context, int, error) {
	return ctx, 0, nil
}
func (h *queryBoundaryHandler) EndRequest(_ context.Context, _ runtime.Stream, _ int, err error, _ runtime.Collect[int], _ runtime.Collect[error]) {
	if err != nil {
		panic(err)
	}
	close(h.ended)
}

// The real Query endpoint and the standard ParallelCall link must not turn
// EndRequest into a downstream-work barrier. No private Query queue is involved.
func TestQueryEndRequestDoesNotWaitForParallelDownstream(t *testing.T) {
	inputCfg := &config.InputStreamConfig{ID: 1, Name: "input", IdService: 1}
	queryCfg := &config.QueryStreamConfig{ID: 2, Name: "query", IdSource: 1, IdService: 1, IdEndpoint: 11}
	mapCfg := &config.MapStreamConfig{ID: 3, Name: "slow", IdSource: 2, IdService: 1}
	cfg := &queryBoundaryConfig{
		queryConfigFixture: queryConfigFixture{
			transformSerdeConfig: transformSerdeConfig{streams: []config.StreamConfig{inputCfg, queryCfg, mapCfg}},
			endpoint:             &config.QueryEndpointConfig{ID: 11, Name: "rows", IdDataConnector: 10, QueryOperation: api.EndpointQueryOperationRows, Enabled: true},
			connector:            &config.PostgreSQLBunDataConnectorConfig{ID: 10, Name: "database", ConnectionString: "postgresql://localhost/unused"},
		},
		links: []*config.LinkConfig{{From: 2, To: 3, CallSemantics: &config.CallSemanticsGroup{ParallelCall: &config.ParallelCallSemanticsConfig{}}}},
	}
	rc, err := config.NewRuntimeConfig(cfg)
	require.NoError(t, err)
	env := &processErrorEnvironment{}
	defer env.parallel.Wait()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	release, unblock := context.WithCancel(ctx)
	defer unblock()
	require.NoError(t, env.InitIsolatedGraphRuntime(rc, env, 1))
	input, err := MakeInputStream[int, any, any](inputCfg, env)
	require.NoError(t, err)
	query, err := MakeQueryStream[int, int, error](queryCfg, input)
	require.NoError(t, err)
	entered, finished := make(chan struct{}), make(chan int, 1)
	_, err = MakeMapStream[int, int](mapCfg, query, MapHandler[int, int](func(actual context.Context, _ runtime.Stream, value int, _ runtime.Collect[int]) {
		close(entered)
		<-release.Done()
		finished <- value
	}))
	require.NoError(t, err)
	handler := &queryBoundaryHandler{ended: make(chan struct{})}
	_, err = dataquery.MakeEndpointConsumer(query, handler, func(call context.Context, _, value int, out runtime.Collect[int]) error {
		out.Out(call, value)
		return nil
	})
	require.NoError(t, err)
	returned := make(chan struct{})
	go func() { input.Consume(ctx, 42); close(returned) }()
	for _, signal := range []<-chan struct{}{entered, handler.ended, returned} {
		select {
		case <-signal:
		case <-ctx.Done():
			t.Fatal("Query did not finalize independently of downstream")
		}
	}
	select {
	case <-finished:
		t.Fatal("downstream finished before release")
	default:
	}
	unblock()
	select {
	case value := <-finished:
		require.Equal(t, 42, value)
	case <-ctx.Done():
		t.Fatal("downstream did not finish")
	}
}
