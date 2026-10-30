package kafka

import (
	"context"
	"errors"

	"github.com/IBM/sarama"
)

// A consumer configured not to reset missing offsets cannot safely resume after
// history loss. Stop its group and preserve the cause in session contexts, even
// when claim construction fails before any message reaches an EndpointHandler.
// Ordinary broker errors retain Sarama's retry/rebalance behavior.
func cancelOnInvalidOffset(err error, resetInvalidOffsets bool, cancel context.CancelCauseFunc) {
	if !resetInvalidOffsets && errors.Is(err, sarama.ErrOffsetOutOfRange) {
		cancel(err)
	}
}

// Keep draining until ConsumerGroup.Close closes Errors, including after
// cancellation. Otherwise a group error send can block shutdown itself.
func drainConsumerErrors(input <-chan error, report func(error)) <-chan struct{} {
	done := make(chan struct{})
	if input == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		for err := range input {
			if err != nil && !errors.Is(err, context.Canceled) {
				report(err)
			}
		}
	}()
	return done
}
