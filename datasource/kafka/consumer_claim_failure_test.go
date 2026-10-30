package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"

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
