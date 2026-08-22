/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	"context"

	"google.golang.org/grpc/stats"
)

// CombineGRPCStatsHandlers composes tracing, metrics and other gRPC stats
// handlers into the single handler accepted by gRPC-Go. Tag callbacks are
// chained so context values produced by one handler are visible to the next.
func CombineGRPCStatsHandlers(handlers ...stats.Handler) stats.Handler {
	filtered := make([]stats.Handler, 0, len(handlers))
	for _, handler := range handlers {
		if handler != nil {
			filtered = append(filtered, handler)
		}
	}
	switch len(filtered) {
	case 0:
		return nil
	case 1:
		return filtered[0]
	default:
		return combinedGRPCStatsHandler(filtered)
	}
}

type combinedGRPCStatsHandler []stats.Handler

func (handlers combinedGRPCStatsHandler) TagRPC(
	ctx context.Context,
	info *stats.RPCTagInfo,
) context.Context {
	for _, handler := range handlers {
		ctx = handler.TagRPC(ctx, info)
	}
	return ctx
}

func (handlers combinedGRPCStatsHandler) HandleRPC(
	ctx context.Context,
	event stats.RPCStats,
) {
	for _, handler := range handlers {
		handler.HandleRPC(ctx, event)
	}
}

func (handlers combinedGRPCStatsHandler) TagConn(
	ctx context.Context,
	info *stats.ConnTagInfo,
) context.Context {
	for _, handler := range handlers {
		ctx = handler.TagConn(ctx, info)
	}
	return ctx
}

func (handlers combinedGRPCStatsHandler) HandleConn(
	ctx context.Context,
	event stats.ConnStats,
) {
	for _, handler := range handlers {
		handler.HandleConn(ctx, event)
	}
}
