package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"google.golang.org/grpc/metadata"
)

type activeRequestEndpoint struct {
	runtime.InputEndpoint
	rejected atomic.Int32
	late     atomic.Int32
	pending  atomic.Int32
}

func (e *activeRequestEndpoint) OnRequestStart(context.Context) time.Time       { return time.Now() }
func (e *activeRequestEndpoint) OnRequestEnd(context.Context, time.Time, error) {}
func (e *activeRequestEndpoint) OnBeginRequestFailed(context.Context, error)    { e.rejected.Add(1) }
func (e *activeRequestEndpoint) OnLateResult(context.Context, string)           { e.late.Add(1) }
func (e *activeRequestEndpoint) OnPendingAdd(context.Context, string)           { e.pending.Add(1) }
func (e *activeRequestEndpoint) OnPendingRemove(context.Context, string)        { e.pending.Add(-1) }

type activeRequestState struct {
	first    bool
	ctx      context.Context
	messages int
}

type activeRequestHandler struct {
	scenario       string
	begins         atomic.Int32
	consumes       atomic.Int32
	callbacks      atomic.Int32
	callbackGate   func()
	endResult      func(error) error
	entered        chan *activeRequestState
	releaseMessage chan struct{}
	endEntered     chan struct{}
	releaseEnd     chan struct{}
	deliver        func(context.Context, string)
	reopen         func(context.Context) error
	reentrantError chan error
}

var errActiveRequestEnd = errors.New("end failed")
var errActiveRequestConsume = errors.New("consume failed")

func (h *activeRequestHandler) BeginRequest(ctx context.Context, _ StreamContext[string, string, error]) (context.Context, *activeRequestState, error) {
	return ctx, &activeRequestState{first: h.begins.Add(1) == 1, ctx: ctx}, nil
}

func (h *activeRequestHandler) ConsumeMessage(ctx context.Context, _ StreamContext[string, string, error], s *activeRequestState, req string, result ResultContext[*activeRequestState, string, string, string, error], sender Sender[string, string]) (context.Context, error) {
	h.consumes.Add(1)
	s.messages++
	result.SetResultCallback("reply", func(context.Context, StreamContext[string, string, error], *activeRequestState, string, Sender[string, string]) bool {
		h.callbacks.Add(1)
		if h.callbackGate != nil {
			h.callbackGate()
		}
		return true
	})
	if s.first && s.messages == 1 {
		h.entered <- s
		<-h.releaseMessage
		if h.scenario == "cancel" {
			return ctx, ctx.Err()
		}
		if h.scenario == "consume_error" {
			return ctx, errActiveRequestConsume
		}
	}
	if req == "last" {
		if err := sender.Send(ctx, "response"); err != nil {
			return ctx, err
		}
		result.Done()
	}
	return ctx, nil
}

func (*activeRequestHandler) GetMessageID(context.Context, StreamContext[string, string, error], *activeRequestState, string) string {
	return "reply"
}
func (*activeRequestHandler) Eof(context.Context, StreamContext[string, string, error], *activeRequestState) {
}

func (h *activeRequestHandler) EndRequest(ctx context.Context, _ StreamContext[string, string, error], err error, s *activeRequestState) error {
	if s.first {
		// A result produced by EndRequest must not enter callbacks or deadlock
		// on the result lock. The ID must still reject another RPC here.
		h.deliver(ctx, "late")
		h.reentrantError <- h.reopen(ctx)
		close(h.endEntered)
		<-h.releaseEnd
		if h.scenario == "end_error" {
			return errActiveRequestEnd
		}
	}
	if h.endResult != nil {
		return h.endResult(err)
	}
	return err
}

type activeRequestTransport struct{ messages []string }

func (s *activeRequestTransport) Recv() (string, error) {
	if len(s.messages) == 0 {
		return "", io.EOF
	}
	value := s.messages[0]
	s.messages = s.messages[1:]
	return value, nil
}
func (*activeRequestTransport) Send(string) error         { return nil }
func (*activeRequestTransport) SendAndClose(string) error { return nil }

type activeRequestLifecycle interface {
	Start(context.Context) error
	Stop(context.Context)
}

func makeActiveRequestTestConsumer(t *testing.T, mode string, hasResult bool, h *activeRequestHandler, endpoint *activeRequestEndpoint) (func(context.Context) error, func(context.Context, string), func(string) bool) {
	t.Helper()
	base := grpcTypedEndpointConsumer[string, string, error]{
		DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[string, string, error](endpoint, nil),
		hasResult:                  hasResult,
	}
	var lifecycle activeRequestLifecycle
	var call func(context.Context) error
	var deliver func(context.Context, string)
	var reserved func(string) bool
	switch mode {
	case "unary":
		ec := &noStreamingEndpointConsumer[*activeRequestState, string, string, string, string, error]{grpcTypedEndpointConsumer: base, handler: h}
		lifecycle = ec
		call = func(ctx context.Context) error { _, err := ec.handle(ctx, "last"); return err }
		deliver = ec.consumeResult
		reserved = func(id string) bool { _, ok := ec.pending.Get(id); return ok }
	case "server":
		ec := &serverStreamingEndpointConsumer[*activeRequestState, string, string, string, string, error]{grpcTypedEndpointConsumer: base, handler: h}
		lifecycle = ec
		call = func(ctx context.Context) error { return ec.handle(ctx, "last", &activeRequestTransport{}) }
		deliver = ec.consumeResult
		reserved = func(id string) bool { _, ok := ec.pending.Get(id); return ok }
	case "client":
		ec := &clientStreamingEndpointConsumer[*activeRequestState, string, string, string, string, error]{grpcTypedEndpointConsumer: base, handler: h}
		lifecycle = ec
		call = func(ctx context.Context) error {
			return ec.handle(ctx, &activeRequestTransport{messages: []string{"first", "last"}})
		}
		deliver = ec.consumeResult
		reserved = func(id string) bool { _, ok := ec.pending.Get(id); return ok }
	case "bidi":
		ec := &bidiStreamingEndpointConsumer[*activeRequestState, string, string, string, string, error]{grpcTypedEndpointConsumer: base, handler: h}
		lifecycle = ec
		call = func(ctx context.Context) error {
			return ec.handle(ctx, &activeRequestTransport{messages: []string{"first", "last"}})
		}
		deliver = ec.consumeResult
		reserved = func(id string) bool { _, ok := ec.pending.Get(id); return ok }
	default:
		t.Fatalf("unknown mode %s", mode)
	}
	if err := lifecycle.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lifecycle.Stop(context.Background()) })
	return call, deliver, reserved
}

func awaitActiveRequest[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("request lifecycle did not progress")
	}
	var zero T
	return zero
}

func TestGRPCSourceActiveRequestReservation(t *testing.T) {
	for _, mode := range []string{"unary", "server", "client", "bidi"} {
		for _, hasResult := range []bool{false, true} {
			for _, scenario := range []string{"success", "end_error", "consume_error", "cancel"} {
				t.Run(fmt.Sprintf("%s/result=%v/%s", mode, hasResult, scenario), func(t *testing.T) {
					h := &activeRequestHandler{
						scenario: scenario,
						entered:  make(chan *activeRequestState, 1), releaseMessage: make(chan struct{}),
						endEntered: make(chan struct{}), releaseEnd: make(chan struct{}),
						reentrantError: make(chan error, 1),
					}
					e := &activeRequestEndpoint{}
					call, deliver, reserved := makeActiveRequestTestConsumer(t, mode, hasResult, h, e)
					h.deliver, h.reopen = deliver, call
					var messageOnce, endOnce sync.Once
					releaseMessage := func() { messageOnce.Do(func() { close(h.releaseMessage) }) }
					releaseEnd := func() { endOnce.Do(func() { close(h.releaseEnd) }) }
					t.Cleanup(func() { releaseMessage(); releaseEnd() })
					ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-stream-id", "active"))
					firstCtx, cancel := context.WithCancel(ctx)
					defer cancel()
					finished := make(chan error, 1)
					go func() { finished <- call(firstCtx) }()
					s := awaitActiveRequest(t, h.entered)
					if !reserved("active") {
						t.Fatal("active ID not reserved")
					}
					deliver(s.ctx, "reply")
					wantCallbacks := int32(0)
					if hasResult {
						wantCallbacks = 1
					}
					if got := h.callbacks.Load(); got != wantCallbacks {
						t.Fatalf("callbacks: got %d, want %d", got, wantCallbacks)
					}
					duplicates := make(chan error, 8)
					for i := 0; i < 8; i++ {
						go func() { duplicates <- call(ctx) }()
					}
					checkDuplicate := func(err error) {
						t.Helper()
						if err == nil || !strings.Contains(err.Error(), "duplicate stream ID") {
							t.Fatalf("expected duplicate rejection, got %v", err)
						}
					}
					for i := 0; i < 8; i++ {
						checkDuplicate(awaitActiveRequest(t, duplicates))
					}
					if got := h.consumes.Load(); got != 1 {
						t.Fatalf("duplicates reached business logic: %d calls", got)
					}
					if !reserved("active") {
						t.Fatal("rejection released the first RPC's ID")
					}
					otherCtx := runtime.WithStreamId(context.Background(), "other")
					if err := call(otherCtx); err != nil {
						t.Fatalf("independent ID rejected: %v", err)
					}
					if scenario == "cancel" {
						cancel()
					}
					releaseMessage()
					awaitActiveRequest(t, h.endEntered)
					checkDuplicate(awaitActiveRequest(t, h.reentrantError))
					checkDuplicate(call(ctx))
					if !reserved("active") {
						t.Fatal("ID released before EndRequest returned")
					}
					if got := h.callbacks.Load(); got != wantCallbacks {
						t.Fatalf("late result reached callback: %d", got)
					}
					releaseEnd()
					err := awaitActiveRequest(t, finished)
					var wantErr error
					switch scenario {
					case "end_error":
						wantErr = errActiveRequestEnd
					case "consume_error":
						wantErr = errActiveRequestConsume
					case "cancel":
						wantErr = context.Canceled
					}
					if !errors.Is(err, wantErr) {
						t.Fatalf("first request: got %v, want %v", err, wantErr)
					}
					if reserved("active") {
						t.Fatal("completed ID retained")
					}
					wantMessages := 1
					if (mode == "client" || mode == "bidi") && (scenario == "success" || scenario == "end_error") {
						wantMessages = 2
					}
					if s.messages != wantMessages {
						t.Fatalf("messages within first RPC: got %d, want %d", s.messages, wantMessages)
					}
					if err := call(ctx); err != nil {
						t.Fatalf("completed ID cannot be reused: %v", err)
					}
					if got := e.rejected.Load(); got != 10 {
						t.Fatalf("rejection metric: got %d, want 10", got)
					}
					if got := e.pending.Load(); got != 0 {
						t.Fatalf("pending metric leaked: %d", got)
					}
				})
			}
		}
	}
}
