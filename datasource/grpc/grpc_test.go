/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package grpc

import (
	"context"
	"testing"

	"github.com/gorundebug/servicelib/runtime"
	"google.golang.org/grpc/metadata"
)

func TestApplyIncomingStreamID(t *testing.T) {
	tests := []struct {
		name     string
		ctx      context.Context
		expected string
		present  bool
	}{
		{
			name: "lowercase metadata",
			ctx: metadata.NewIncomingContext(
				context.Background(), metadata.MD{"x-stream-id": {"from-metadata"}},
			),
			expected: "from-metadata",
			present:  true,
		},
		{
			name: "case insensitive metadata",
			ctx: metadata.NewIncomingContext(
				context.Background(), metadata.MD{"X-Stream-ID": {"from-uppercase-metadata"}},
			),
			expected: "from-uppercase-metadata",
			present:  true,
		},
		{
			name: "existing stream id wins",
			ctx: metadata.NewIncomingContext(
				runtime.WithStreamId(context.Background(), "existing"),
				metadata.MD{"x-stream-id": {"from-metadata"}},
			),
			expected: "existing",
			present:  true,
		},
		{
			name:    "missing metadata",
			ctx:     context.Background(),
			present: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			streamID, present := runtime.StreamIdFromContext(applyIncomingStreamID(test.ctx))
			if present != test.present {
				t.Fatalf("stream id presence = %v, want %v", present, test.present)
			}
			if present && streamID.GetID() != test.expected {
				t.Fatalf("stream id = %q, want %q", streamID.GetID(), test.expected)
			}
		})
	}
}
