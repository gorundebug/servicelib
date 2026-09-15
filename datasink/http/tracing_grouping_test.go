package http

import (
	"context"
	"io"
	nethttp "net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

type groupingSinkStream struct {
	runtime.TypedSinkStreamWithResult[int, int, error]
	tracingAllowed bool
	reads          int
}

func (s *groupingSinkStream) label(value string) string {
	if !s.tracingAllowed {
		panic("request path read cached stream metadata")
	}
	s.reads++
	return value
}
func (s *groupingSinkStream) GetName() string          { return s.label("Publish Booking") }
func (s *groupingSinkStream) GetPipelineName() string  { return s.label("booking") }
func (s *groupingSinkStream) GetComponentName() string { return s.label("Reserve Inventory") }
func (*groupingSinkStream) GetConfig() config.StreamConfig {
	panic("transport tracing read configuration")
}

type groupingSinkEndpoint struct {
	runtime.SinkEndpoint
	started, finished int
	tracingAllowed    bool
	reads             int
}

func (e *groupingSinkEndpoint) GetName() string {
	if !e.tracingAllowed {
		panic("request path read cached endpoint metadata")
	}
	e.reads++
	return "Inventory API"
}
func (e *groupingSinkEndpoint) OnRequestStart(context.Context) time.Time {
	e.started++
	return time.Time{}
}
func (e *groupingSinkEndpoint) OnRequestEnd(context.Context, time.Time, error) { e.finished++ }

type groupingHandler struct{ calls int }

func (h *groupingHandler) BeginRequest(ctx context.Context, _ StreamContext[int, int, error]) (context.Context, int, error) {
	h.calls++
	return ctx, 0, nil
}
func (h *groupingHandler) ConsumeMessage(ctx context.Context, _ StreamContext[int, int, error], _ int, _ int, req *Requester) error {
	h.calls++
	_, err := req.NewRequest(ctx, "POST", "http://inventory.test/reserve", nil)
	return err
}
func (h *groupingHandler) HandleResponse(context.Context, StreamContext[int, int, error], int, Response) error {
	h.calls++
	return nil
}
func (h *groupingHandler) EndRequest(context.Context, StreamContext[int, int, error], error, int) {
	h.calls++
}

type groupingClient struct{ calls int }

func (c *groupingClient) Do(*nethttp.Request) (*nethttp.Response, error) {
	c.calls++
	return &nethttp.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("reserved"))}, nil
}

type groupingSpan struct {
	tracing.Span
	ends int
}

func (s *groupingSpan) End()                                { s.ends++ }
func (*groupingSpan) AddEvent(string, ...tracing.Attribute) {}

type groupingTracer struct {
	name  string
	attrs map[string]string
	span  groupingSpan
}

func (t *groupingTracer) Start(ctx context.Context, name string, attrs ...tracing.Attribute) (context.Context, tracing.Span) {
	t.name = name
	t.attrs = make(map[string]string, len(attrs))
	for _, attr := range attrs {
		t.attrs[attr.Key] = attr.Value.AsString()
	}
	return ctx, &t.span
}

func TestHTTPSinkGroupingAndDisabledTracingUseActualConsumePath(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		tracerPresent, sampled, replay bool
	}{
		{"enabled", true, true, false}, {"unsampled", true, false, false},
		{"no tracer", false, true, false}, {"replay", true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enabled := tc.tracerPresent && tc.sampled && !tc.replay
			stream := &groupingSinkStream{tracingAllowed: tc.tracerPresent}
			endpoint := &groupingSinkEndpoint{tracingAllowed: tc.tracerPresent}
			handler, client := &groupingHandler{}, &groupingClient{}
			recorder := &groupingTracer{}
			consumer := netHTTPSinkEndpointTypedConsumer[int, int, int, int, int, error]{stream: stream, endpoint: endpoint, handler: handler, client: client}
			if tc.tracerPresent {
				consumer.tracer = recorder
				consumer.spanAttributes = runtime.MakeEndpointSpanAttributes(stream, endpoint)
				if stream.reads != 3 || endpoint.reads != 1 {
					t.Fatal("constructor must capture each static label exactly once")
				}
			}
			stream.tracingAllowed, endpoint.tracingAllowed = false, false
			stream.reads, endpoint.reads = 0, 0
			ctx := context.Background()
			if tc.sampled {
				ctx = tracing.EnableSampling(ctx)
			}
			if tc.replay {
				ctx = tracing.WithoutRecording(ctx)
			}
			consumer.Consume(ctx, 42)
			consumer.Consume(ctx, 43)
			if handler.calls != 8 || client.calls != 2 || endpoint.started != 2 || endpoint.finished != 2 {
				t.Fatalf("transport lifecycle changed: handler=%d client=%d starts=%d ends=%d", handler.calls, client.calls, endpoint.started, endpoint.finished)
			}
			if stream.reads != 0 || endpoint.reads != 0 {
				t.Fatal("request path must not read static metadata, even when sampled")
			}
			if !enabled {
				if recorder.name != "" || stream.reads != 0 {
					t.Fatal("disabled tracing performed work")
				}
				return
			}
			if recorder.name != "http.output" || recorder.span.ends != 2 {
				t.Fatal("expected one completed HTTP output span per request")
			}
			expected := map[string]string{"stream": "Publish Booking", "endpoint": "Inventory API", "pipeline": "booking", "component": "Reserve Inventory"}
			if len(recorder.attrs) != len(expected) {
				t.Fatalf("unexpected attributes: %v", recorder.attrs)
			}
			for key, value := range expected {
				if recorder.attrs[key] != value {
					t.Errorf("%s = %v, expected %s", key, recorder.attrs[key], value)
				}
			}
		})
	}
}
