package contextvalue

import "context"

// StreamID is the transport-independent stream identity propagated through a
// request context.
type StreamID interface {
	GetID() string
}

type streamID struct {
	id string
}

func (id *streamID) GetID() string {
	return id.id
}

type streamIDKey struct{}

// WithStreamID attaches a stream identity without importing the runtime root
// package. Transport middleware and graph operators therefore share one key.
func WithStreamID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, streamIDKey{}, &streamID{id: id})
}

// WithStreamIDInspected records that transport metadata has already been
// checked and contains no stream identity. Downstream adapters can then avoid
// parsing the same metadata again.
func WithStreamIDInspected(ctx context.Context) context.Context {
	if StreamIDInspected(ctx) {
		return ctx
	}
	return context.WithValue(ctx, streamIDKey{}, (*streamID)(nil))
}

// StreamIDInspected reports whether an upstream transport boundary has
// resolved the stream identity, including the case where no identity exists.
func StreamIDInspected(ctx context.Context) bool {
	_, ok := ctx.Value(streamIDKey{}).(*streamID)
	return ok
}

// StreamIDFromContext returns the stream identity previously attached by a
// transport or graph boundary.
func StreamIDFromContext(ctx context.Context) (StreamID, bool) {
	id, ok := ctx.Value(streamIDKey{}).(*streamID)
	if !ok || id == nil {
		return nil, false
	}
	return id, true
}
