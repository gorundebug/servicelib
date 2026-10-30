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

type blockedProducer struct {
	sarama.AsyncProducer
	input   chan *sarama.ProducerMessage
	entered chan struct{}
	once    sync.Once
	closes  atomic.Int32
}

func (p *blockedProducer) Input() chan<- *sarama.ProducerMessage {
	p.once.Do(func() { close(p.entered) })
	return p.input
}
func (p *blockedProducer) AsyncClose() {
	if p.closes.Add(1) == 1 {
		close(p.input)
	}
}
func TestProducerStopCancelsBlockedEnqueue(t *testing.T) {
	p := &blockedProducer{input: make(chan *sarama.ProducerMessage), entered: make(chan struct{})}
	ds := &saramaKafkaDataSink{OutputDataSink: &runtime.OutputDataSink{}, producer: p}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	completed := make(chan error, 1)
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		ds.SendMessage(ctx, &sarama.ProducerMessage{Metadata: &messageMetadata{onDelivery: func(_ int32, _ int64, err error) { completed <- err }}})
	}()
	<-p.entered
	ds.Stop(t.Context())
	select {
	case err := <-completed:
		if err == nil {
			t.Error("stopped enqueue was reported delivered")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Stop left an enqueue blocked against a closed producer")
		cancel()
		<-completed
	}
	<-returned
	ds.Stop(t.Context())
	if p.closes.Load() != 1 {
		t.Fatalf("producer closed %d times", p.closes.Load())
	}
}
func TestProducerBlockedEnqueueRespectsDeadline(t *testing.T) {
	p := &blockedProducer{input: make(chan *sarama.ProducerMessage), entered: make(chan struct{})}
	ds := &saramaKafkaDataSink{OutputDataSink: &runtime.OutputDataSink{}, producer: p}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()
	calls := 0
	ds.SendMessage(ctx, &sarama.ProducerMessage{Metadata: &messageMetadata{onDelivery: func(_ int32, _ int64, err error) {
		calls++
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Error(err)
		}
	}}})
	if calls != 1 {
		t.Fatal(calls)
	}
	ds.Stop(t.Context())
}

func TestProducerConcurrentStopRejectsEveryBlockedSender(t *testing.T) {
	p := &blockedProducer{input: make(chan *sarama.ProducerMessage), entered: make(chan struct{})}
	ds := &saramaKafkaDataSink{OutputDataSink: &runtime.OutputDataSink{}, producer: p}
	var sends, stops sync.WaitGroup
	var callbacks atomic.Int32
	for i := 0; i < 32; i++ {
		sends.Add(1)
		go func() {
			defer sends.Done()
			ds.SendMessage(t.Context(), &sarama.ProducerMessage{Metadata: &messageMetadata{onDelivery: func(_ int32, _ int64, err error) {
				callbacks.Add(1)
				if err == nil {
					t.Error("unsent message acknowledged")
				}
			}}})
		}()
	}
	<-p.entered
	for i := 0; i < 4; i++ {
		stops.Add(1)
		go func() { defer stops.Done(); ds.Stop(t.Context()) }()
	}
	sends.Wait()
	stops.Wait()
	if callbacks.Load() != 32 || p.closes.Load() != 1 {
		t.Fatal(callbacks.Load(), p.closes.Load())
	}
}
