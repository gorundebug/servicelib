package kafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type admissionHandler struct {
	requestLifecycleHandler
	limit   int
	entered chan struct{}
}

func (h *admissionHandler) Concurrency(StreamContext[int, int, error]) int {
	if h.entered != nil {
		select {
		case h.entered <- struct{}{}:
		default:
		}
	}
	return h.limit
}

func TestKafkaConcurrencyRejectsNegativeAndPreservesZero(t *testing.T) {
	for _, limit := range []int{-1, 0, 2} {
		ec := &saramaKafkaTypedEndpointConsumer[int, int, int, error]{handler: &admissionHandler{limit: limit}}
		ec.concCond = sync.NewCond(&ec.concMu)
		err := ec.acquireConcurrency(t.Context())
		if limit < 0 {
			if err == nil || !strings.Contains(err.Error(), "non-negative") || ec.active != 0 {
				t.Fatalf("invalid limit admitted: %v active=%d", err, ec.active)
			}
		} else {
			if err != nil || ec.active != 1 {
				t.Fatalf("valid admission: %v active=%d", err, ec.active)
			}
			ec.releaseConcurrency()
		}
	}
}

func TestKafkaContendedAdmissionWakesOnSessionCancellation(t *testing.T) {
	entered := make(chan struct{}, 1)
	ec := &saramaKafkaTypedEndpointConsumer[int, int, int, error]{handler: &admissionHandler{limit: 1, entered: entered}, active: 1}
	ec.concCond = sync.NewCond(&ec.concMu)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ec.acquireConcurrency(ctx) }()
	// Cancel after the waiter has passed its initial context check. This covers
	// cancellation while entering Cond.Wait, rather than only pre-cancellation.
	<-entered
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected admission result: %v", err)
		}
	case <-time.After(time.Second):
		ec.concMu.Lock()
		ec.stopped = true
		ec.concCond.Broadcast()
		ec.concMu.Unlock()
		t.Fatal("cancelled Kafka session remained stuck behind an occupied slot")
	}
	if ec.active != 1 {
		t.Fatal("cancelled waiter changed active ownership")
	}
}
