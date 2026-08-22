package runtime

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/stats"
)

type statsContextKey string

type recordingStatsHandler struct {
	name   string
	events *[]string
	t      *testing.T
}

func (handler recordingStatsHandler) TagRPC(
	ctx context.Context,
	_ *stats.RPCTagInfo,
) context.Context {
	*handler.events = append(*handler.events, "tag-rpc:"+handler.name)
	return context.WithValue(ctx, statsContextKey(handler.name), true)
}

func (handler recordingStatsHandler) HandleRPC(
	ctx context.Context,
	_ stats.RPCStats,
) {
	require.Equal(handler.t, true, ctx.Value(statsContextKey(handler.name)))
	*handler.events = append(*handler.events, "handle-rpc:"+handler.name)
}

func (handler recordingStatsHandler) TagConn(
	ctx context.Context,
	_ *stats.ConnTagInfo,
) context.Context {
	*handler.events = append(*handler.events, "tag-conn:"+handler.name)
	return context.WithValue(ctx, statsContextKey(handler.name), true)
}

func (handler recordingStatsHandler) HandleConn(
	ctx context.Context,
	_ stats.ConnStats,
) {
	require.Equal(handler.t, true, ctx.Value(statsContextKey(handler.name)))
	*handler.events = append(*handler.events, "handle-conn:"+handler.name)
}

func TestCombineGRPCStatsHandlers(t *testing.T) {
	var events []string
	combined := CombineGRPCStatsHandlers(
		recordingStatsHandler{name: "tracing", events: &events, t: t},
		nil,
		recordingStatsHandler{name: "metrics", events: &events, t: t},
	)
	require.NotNil(t, combined)

	rpcContext := combined.TagRPC(context.Background(), &stats.RPCTagInfo{})
	combined.HandleRPC(rpcContext, &stats.Begin{})
	connContext := combined.TagConn(context.Background(), &stats.ConnTagInfo{})
	combined.HandleConn(connContext, &stats.ConnBegin{})

	require.Equal(t, []string{
		"tag-rpc:tracing",
		"tag-rpc:metrics",
		"handle-rpc:tracing",
		"handle-rpc:metrics",
		"tag-conn:tracing",
		"tag-conn:metrics",
		"handle-conn:tracing",
		"handle-conn:metrics",
	}, events)
}

func TestCombineGRPCStatsHandlersEmptyAndSingle(t *testing.T) {
	require.Nil(t, CombineGRPCStatsHandlers(nil))
	handler := &recordingStatsHandler{name: "single", events: &[]string{}, t: t}
	require.Same(t, handler, CombineGRPCStatsHandlers(handler))
}
