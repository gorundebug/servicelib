package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/gorundebug/servicelib/runtime"
)

// Exercise the real typed endpoint, pending store and result dispatcher. Only
// broker I/O and telemetry are substituted; this is not live-broker acceptance.
type requestLifecycleEndpoint struct {
	runtime.InputEndpoint
	starts, ends, adds, removes, late, unknown atomic.Int32
}

func (*requestLifecycleEndpoint) GetRuntimeEnvironment() runtime.RuntimeEnvironment { return nil }
func (e *requestLifecycleEndpoint) OnRequestStart(context.Context) time.Time {
	e.starts.Add(1)
	return time.Now()
}
func (e *requestLifecycleEndpoint) OnRequestEnd(context.Context, time.Time, error) { e.ends.Add(1) }
func (e *requestLifecycleEndpoint) OnPendingAdd(context.Context, string)           { e.adds.Add(1) }
func (e *requestLifecycleEndpoint) OnPendingRemove(context.Context, string)        { e.removes.Add(1) }
func (e *requestLifecycleEndpoint) OnLateResult(context.Context, string)           { e.late.Add(1) }
func (e *requestLifecycleEndpoint) OnUnknownMessageID(context.Context, string, string) {
	e.unknown.Add(1)
}

type requestLifecycleSession struct {
	lifecycleSession
	marks, commits atomic.Int32
}

func (s *requestLifecycleSession) MarkMessage(*sarama.ConsumerMessage, string) { s.marks.Add(1) }
func (s *requestLifecycleSession) Commit()                                     { s.commits.Add(1) }

type requestLifecycleHandler struct {
	begin, consumed, callbacks, end atomic.Int32
	started                         chan context.Context
	ended                           chan error
	consumeError                    error
}

func (*requestLifecycleHandler) Concurrency(StreamContext[int, int, error]) int { return 1 }
func (*requestLifecycleHandler) OnError(ctx context.Context, sc StreamContext[int, int, error], err error) {
	sc.ErrorCollect(ctx, err)
}

func (h *requestLifecycleHandler) BeginRequest(ctx context.Context, _ StreamContext[int, int, error]) (context.Context, int, error) {
	h.begin.Add(1)
	return runtime.WithStreamId(ctx, "request-session"), 1, nil
}
func (h *requestLifecycleHandler) ConsumeMessage(ctx context.Context, _ StreamContext[int, int, error], state int, message ConsumerMessage, result ResultContext[int, int, int, error]) error {
	h.consumed.Add(1)
	if state != 1 || message.Offset != 7 {
		return errors.New("unexpected request state or offset")
	}
	result.SetResultCallback("request", func(_ context.Context, _ StreamContext[int, int, error], got int, value int) bool {
		if got != state || value != 42 {
			return false
		}
		h.callbacks.Add(1)
		result.Done()
		return true
	})
	h.started <- ctx
	return h.consumeError
}
func (*requestLifecycleHandler) GetMessageID(_ context.Context, _ StreamContext[int, int, error], _ int, value int) string {
	if value != 42 {
		return "foreign"
	}
	return "request"
}
func (h *requestLifecycleHandler) EndRequest(_ context.Context, _ StreamContext[int, int, error], err error, _ int) {
	h.end.Add(1)
	h.ended <- err
}

func TestConsumerRequestEndpointLifecycle(t *testing.T) {
	for _, mode := range []string{"result", "cancel", "cancel-cause", "consume-error", "result-and-cancel", "no-result"} {
		t.Run(mode, func(t *testing.T) {
			owner, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			ctx, cancel := context.WithCancelCause(owner)
			defer cancel(nil)
			failure := errors.New("request rejected")
			h := &requestLifecycleHandler{
				started: make(chan context.Context, 1), ended: make(chan error, 1),
			}
			if mode == "consume-error" {
				h.consumeError = failure
			}
			ep := &requestLifecycleEndpoint{}
			ec := &saramaKafkaTypedEndpointConsumer[int, int, int, error]{
				DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[int, int, error](ep, nil),
				handler:                    h, hasResult: mode != "no-result",
			}
			ec.concCond = sync.NewCond(&ec.concMu)
			if err := ec.Start(owner); err != nil {
				t.Fatal(err)
			}
			defer ec.Stop(owner)
			session := &requestLifecycleSession{lifecycleSession: lifecycleSession{ctx: ctx}}
			done := make(chan struct{})
			var requestErr error
			go func() {
				defer close(done)
				requestErr = ec.EndpointRequest(session, lifecycleClaim{}, &sarama.ConsumerMessage{Topic: "events", Partition: 1, Offset: 7})
			}()
			var request context.Context
			select {
			case request = <-h.started:
			case <-owner.Done():
				t.Fatal("request was not dispatched", owner.Err())
			}
			var want error
			switch mode {
			case "result", "result-and-cancel":
				ec.consumeResult(request, -1)
				if ep.unknown.Load() != 1 {
					t.Fatal("foreign result did not use normal SDK dispatch")
				}
				select {
				case <-done:
					t.Fatal("request completed before its result")
				default:
				}
				ec.consumeResult(request, 42)
				if mode == "result-and-cancel" {
					cancel(nil)
				}
			case "cancel":
				cancel(nil)
				want = context.Canceled
			case "cancel-cause":
				cancel(failure)
				want = failure
			case "consume-error":
				want = failure
			}
			select {
			case <-done:
			case <-owner.Done():
				t.Fatal("request request did not finish", owner.Err())
			}
			if !errors.Is(requestErr, want) {
				t.Fatal("claim lost the request outcome", requestErr, want)
			}
			select {
			case err := <-h.ended:
				if !errors.Is(err, want) {
					t.Fatal("EndRequest lost the outcome", err, want)
				}
			default:
				t.Fatal("missing EndRequest")
			}
			if h.begin.Load() != 1 || h.consumed.Load() != 1 || h.end.Load() != 1 || ep.starts.Load() != 1 || ep.ends.Load() != 1 {
				t.Fatal("request did not use exactly one request lifecycle")
			}
			if session.marks.Load() != 0 || session.commits.Load() != 0 {
				t.Fatal("SDK acknowledged transactional input without the owner")
			}
			ec.concMu.Lock()
			active := ec.active
			ec.concMu.Unlock()
			if active != 0 {
				t.Fatal("concurrency slot leaked", active)
			}
			if ec.hasResult {
				if _, exists := ec.pending.Get("request-session"); exists || ep.adds.Load() != 1 || ep.removes.Load() != 1 {
					t.Fatal("request pending entry leaked")
				}
				before := h.callbacks.Load()
				ec.consumeResult(request, 42)
				if ep.late.Load() != 1 || h.callbacks.Load() != before {
					t.Fatal("late result reached a finished request")
				}
			} else if ep.adds.Load() != 0 || ep.removes.Load() != 0 {
				t.Fatal("no-result request allocated pending state")
			}
		})
	}
}
