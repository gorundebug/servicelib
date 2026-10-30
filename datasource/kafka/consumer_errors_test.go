package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestKafkaInvalidOffsetCancellationPolicy(t *testing.T) {
	for _, test := range []struct {
		name           string
		err            error
		reset, stopped bool
	}{
		{"lost-history", sarama.ErrOffsetOutOfRange, false, true},
		{"wrapped-history-loss", fmt.Errorf("claim: %w", sarama.ErrOffsetOutOfRange), false, true},
		{"reset-enabled", sarama.ErrOffsetOutOfRange, true, false},
		{"temporary-broker-failure", sarama.ErrNotLeaderForPartition, false, false},
		{"ordinary-error", errors.New("broker unavailable"), false, false},
		{"revocation", context.Canceled, false, false},
		{"no-error", nil, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			session, endSession := context.WithCancelCause(ctx)
			defer endSession(nil)
			cancelOnInvalidOffset(test.err, test.reset, cancel)
			if (ctx.Err() != nil) != test.stopped {
				t.Fatal("incorrect group cancellation", context.Cause(ctx))
			}
			if test.stopped && (!errors.Is(context.Cause(session), sarama.ErrOffsetOutOfRange) || context.Cause(ctx) != test.err) {
				t.Fatal("lost transport failure cause", context.Cause(ctx), context.Cause(session))
			}
		})
	}
}

func TestKafkaInvalidOffsetPreservesEarlierShutdown(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(context.Canceled)
	cancelOnInvalidOffset(sarama.ErrOffsetOutOfRange, false, cancel)
	if context.Cause(ctx) != context.Canceled {
		t.Fatal("late broker error replaced shutdown cause", context.Cause(ctx))
	}
}

func TestConsumerErrorsDrainThroughShutdown(t *testing.T) {
	input := make(chan error)
	var reported []error
	done := drainConsumerErrors(input, func(err error) { reported = append(reported, err) })
	first, last := errors.New("consume failed"), errors.New("close failed")
	go func() {
		defer close(input)
		input <- first
		input <- context.Canceled
		input <- nil
		input <- last
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("consumer error channel was not drained")
	}
	if len(reported) != 2 || reported[0] != first || reported[1] != last {
		t.Fatal("consumer errors lost or cancellation reported", reported)
	}
	select {
	case <-drainConsumerErrors(nil, func(error) { t.Fatal("nil input reported an error") }):
	default:
		t.Fatal("nil input did not finish synchronously")
	}
}
