package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/gorundebug/servicelib/runtime"
)

// A normal generated handler must not need ConsumerLifecycle to keep a failed
// SQL/message operation from being overtaken by the next offset's ACK.
func TestOrdinaryConsumerClaimStopsOnRequestFailure(t *testing.T) {
	want := errors.New("database transaction failed")
	h := &requestLifecycleHandler{started: make(chan context.Context, 2), ended: make(chan error, 2), consumeError: want}
	metrics := &requestLifecycleEndpoint{}
	ec := &saramaKafkaTypedEndpointConsumer[int, int, int, error]{
		DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[int, int, error](metrics, nil),
		handler:                    h, hasResult: true,
	}
	ec.concCond = sync.NewCond(&ec.concMu)
	if err := ec.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer ec.Stop(t.Context())
	input := make(chan *sarama.ConsumerMessage, 2)
	input <- &sarama.ConsumerMessage{Topic: "events", Partition: 1, Offset: 7}
	input <- &sarama.ConsumerMessage{Topic: "events", Partition: 1, Offset: 9}
	close(input)
	endpoint := &saramaKafkaEndpoint{consumer: ec}
	session := &requestLifecycleSession{lifecycleSession: lifecycleSession{ctx: t.Context()}}
	if err := endpoint.ConsumeClaim(session, lifecycleClaim{messages: input}); !errors.Is(err, want) || h.consumed.Load() != 1 || len(input) != 1 {
		t.Fatalf("advanced after ordinary handler failure: calls=%d pending=%d err=%v", h.consumed.Load(), len(input), err)
	}
	if session.marks.Load() != 0 || session.commits.Load() != 0 {
		t.Fatal("failed request acknowledged")
	}
}

type asynchronousClaimFailureHandler struct {
	requestLifecycleHandler
	cancel context.CancelCauseFunc
}

func (h *asynchronousClaimFailureHandler) BeginRequest(ctx context.Context, sc StreamContext[int, int, error]) (context.Context, int, error) {
	ctx, state, err := h.requestLifecycleHandler.BeginRequest(ctx, sc)
	ctx, h.cancel = context.WithCancelCause(ctx)
	return ctx, state, err
}

func (h *asynchronousClaimFailureHandler) ConsumeMessage(ctx context.Context, _ StreamContext[int, int, error], _ int, message ConsumerMessage, result ResultContext[int, int, int, error]) error {
	h.consumed.Add(1)
	if message.Offset == 7 {
		// Report failure through result dispatch, independently of this method's
		// nil return. Only the request is cancelled, not the live session.
		result.SetResultCallback("request", func(context.Context, StreamContext[int, int, error], int, int) bool {
			h.callbacks.Add(1)
			h.cancel(h.consumeError)
			return true
		})
		h.started <- ctx
		return nil
	}
	// If the claim incorrectly continues, this later cumulative ACK would
	// cover the failed offset. Make that consequence observable.
	message.MarkMessage("")
	message.Commit()
	result.Done()
	return nil
}

func TestConsumerClaimStopsOnAsynchronousFailureBeforeLaterAcknowledgment(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	want := errors.New("asynchronous database transaction failed")
	h := &asynchronousClaimFailureHandler{requestLifecycleHandler: requestLifecycleHandler{
		started: make(chan context.Context, 1), ended: make(chan error, 2), consumeError: want,
	}}
	metrics := &requestLifecycleEndpoint{}
	ec := &saramaKafkaTypedEndpointConsumer[int, int, int, error]{
		DataSourceEndpointConsumer: runtime.MakeDataSourceEndpointConsumer[int, int, error](metrics, nil),
		handler:                    h, hasResult: true,
	}
	ec.concCond = sync.NewCond(&ec.concMu)
	if err := ec.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ec.Stop(ctx)
	input := make(chan *sarama.ConsumerMessage, 2)
	input <- &sarama.ConsumerMessage{Topic: "events", Partition: 1, Offset: 7}
	input <- &sarama.ConsumerMessage{Topic: "events", Partition: 1, Offset: 9}
	close(input)
	session := &requestLifecycleSession{lifecycleSession: lifecycleSession{ctx: ctx}}
	endpoint := &saramaKafkaEndpoint{consumer: ec}
	finished := make(chan error, 1)
	go func() { finished <- endpoint.ConsumeClaim(session, lifecycleClaim{messages: input}) }()
	select {
	case request := <-h.started:
		ec.consumeResult(request, 42)
	case <-ctx.Done():
		t.Fatal("request did not start", ctx.Err())
	}
	select {
	case err := <-finished:
		if !errors.Is(err, want) {
			t.Fatal("claim lost asynchronous failure", err)
		}
	case <-ctx.Done():
		t.Fatal("claim did not stop", ctx.Err())
	}
	if session.Context().Err() != nil || h.consumed.Load() != 1 || h.callbacks.Load() != 1 || len(input) != 1 {
		t.Fatal("failure did not stop the live claim before its next record")
	}
	if h.end.Load() != 1 || !errors.Is(<-h.ended, want) {
		t.Fatal("request finalization lost asynchronous failure")
	}
	if session.marks.Load() != 0 || session.commits.Load() != 0 {
		t.Fatal("later acknowledgment covered the failed record")
	}
}
