package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
)

type contractErrorStream struct {
	runtime.TypedSerializedStream[int]
	environment.ServiceEnvironment
}

func (s *contractErrorStream) GetEnvironment() environment.ServiceEnvironment { return s }
func (*contractErrorStream) Log() (logger log.Logger)                         { return }

type contractCollector[T any] func(context.Context, T)

func (f contractCollector[T]) Out(ctx context.Context, value T) { f(ctx, value) }

// This fixture deliberately implements only the existing EndpointHandler.
// No optional lifecycle, stop or batch capability.
type ordinaryErrorHandler struct {
	failure                error
	begin, consumed, ended int
	transport              int
}

func (h *ordinaryErrorHandler) OnError(ctx context.Context, sc StreamContext[int, int, error], err error) {
	h.transport++
	sc.ErrorCollect(ctx, err)
}

func (*ordinaryErrorHandler) Concurrency(StreamContext[int, int, error]) int { return 1 }
func (h *ordinaryErrorHandler) BeginRequest(ctx context.Context, _ StreamContext[int, int, error]) (context.Context, int, error) {
	h.begin++
	return ctx, h.begin, nil
}
func (h *ordinaryErrorHandler) ConsumeMessage(ctx context.Context, sc StreamContext[int, int, error], state int, message ConsumerMessage, _ ResultContext[int, int, int, error]) error {
	h.consumed++
	if state == 1 {
		return h.failure
	}
	sc.Collect(ctx, int(message.Offset))
	return nil
}
func (*ordinaryErrorHandler) GetMessageID(context.Context, StreamContext[int, int, error], int, int) string {
	return ""
}
func (h *ordinaryErrorHandler) EndRequest(ctx context.Context, sc StreamContext[int, int, error], err error, _ int) {
	h.ended++
	if err != nil {
		sc.ErrorCollect(ctx, err)
	}
}

func TestOrdinaryKafkaHandlerRoutesRequestErrorWithoutCapabilities(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	want := errors.New("ordinary message processing failed")
	handler := &ordinaryErrorHandler{failure: want}
	var received []error
	var values []int
	sc := runtime.MakeStreamContext[int, int, error](&contractErrorStream{}, nil,
		contractCollector[int](func(_ context.Context, value int) { values = append(values, value) }),
		contractCollector[error](func(errorContext context.Context, err error) {
			if errorContext.Err() != nil {
				t.Error("error stream received a cancelled context")
			}
			received = append(received, err)
		}))
	metrics := &requestLifecycleEndpoint{}
	consumer := &saramaKafkaTypedEndpointConsumer[int, int, int, error]{
		DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[int, int, error](metrics, nil),
		handler:                    handler, sc: sc,
	}
	consumer.concCond = sync.NewCond(&consumer.concMu)
	if err := consumer.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer consumer.Stop(ctx)
	session := &requestLifecycleSession{lifecycleSession: lifecycleSession{ctx: ctx}}
	messages := make(chan *sarama.ConsumerMessage, 2)
	messages <- &sarama.ConsumerMessage{Topic: "events", Partition: 0, Offset: 42}
	messages <- &sarama.ConsumerMessage{Topic: "events", Partition: 0, Offset: 43}
	close(messages)
	endpoint := &saramaKafkaEndpoint{consumer: consumer}
	if err := endpoint.ConsumeClaim(session, lifecycleClaim{messages: messages}); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if handler.begin != 1 || handler.consumed != 1 || handler.ended != 1 || len(messages) != 1 {
		t.Fatalf("ordinary request lifecycle changed: %+v", handler)
	}
	if len(received) != 1 || !errors.Is(received[0], want) || len(values) != 0 {
		t.Fatal("ordinary error/data routing failed", received, values)
	}
	if session.marks.Load() != 0 || session.commits.Load() != 0 || ctx.Err() != nil {
		t.Fatal("reporting an error implicitly acknowledged input or stopped its owner")
	}
	if metrics.ends.Load() != 1 {
		t.Fatal("ordinary handler enabled an implicit execution mode")
	}
	// A fresh broker assignment can replay the failed offset; the healthy
	// request then permits the next message using the same ordinary contract.
	replay := make(chan *sarama.ConsumerMessage, 2)
	replay <- &sarama.ConsumerMessage{Topic: "events", Partition: 0, Offset: 42}
	replay <- &sarama.ConsumerMessage{Topic: "events", Partition: 0, Offset: 43}
	close(replay)
	if err := endpoint.ConsumeClaim(session, lifecycleClaim{messages: replay}); err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || values[0] != 42 || values[1] != 43 || handler.ended != 3 {
		t.Fatal("fresh assignment did not replay", values)
	}
}

func TestKafkaTransportErrorWithoutMessageState(t *testing.T) {
	handler := &ordinaryErrorHandler{}
	var received []error
	sc := runtime.MakeStreamContext[int, int, error](&contractErrorStream{}, nil, nil,
		contractCollector[error](func(ctx context.Context, err error) {
			if ctx.Err() != nil {
				t.Error("transport error lost its service context")
			}
			received = append(received, err)
		}))
	consumer := &saramaKafkaTypedEndpointConsumer[int, int, int, error]{handler: handler, sc: sc}
	// A claim can finish before the group error is delivered. Its cancellation
	// cause must not be used as a substitute for the actual transport error.
	session, endSession := context.WithCancelCause(t.Context())
	endSession(sarama.ErrSessionConsumeClaimExited)
	consumer.EndpointError(t.Context(), sarama.ErrOffsetOutOfRange)
	consumer.EndpointError(t.Context(), context.Canceled)
	consumer.EndpointError(session, sarama.ErrOffsetOutOfRange)
	consumer.EndpointError(t.Context(), nil)
	if handler.transport != 1 || handler.begin != 0 || handler.consumed != 0 || handler.ended != 0 ||
		len(received) != 1 || !errors.Is(received[0], sarama.ErrOffsetOutOfRange) {
		t.Fatal("transport error was lost or fabricated a message lifecycle", received, handler)
	}
}
