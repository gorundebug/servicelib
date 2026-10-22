package kafka

import (
	"context"
	"github.com/IBM/sarama"
)

// ConsumerLifecycle is an optional capability of an EndpointHandler. The normal
// generated handler contract is unchanged. These hooks belong to endpoint/service
// composition, not to business functions processing individual stream values.
//
// ConfigureConsumer runs once before starting this endpoint's consumer; it can
// select isolation, disable automatic commits or reject unsupported settings.
// SetupConsumer runs on every group assignment, before any ConsumeClaim. It may
// validate exclusive ownership and reset assigned offsets to an authoritative
// snapshot cursor. Returning an error prevents processing that assignment.
// CleanupConsumer runs after claims have stopped; it should revoke readiness.
//
// ConsumerError is checked between synchronous EndpointRequest invocations. A
// latched error stops the claim instead of continuing past a failed message. It
// can be called concurrently by different partitions and must be thread-safe.
// Hooks must honor session cancellation. Neither this interface nor the runtime
// automatically marks/commits messages or clears a handler's failure state.
type ConsumerLifecycle interface {
	ConfigureConsumer(*sarama.Config) error
	SetupConsumer(sarama.ConsumerGroupSession) error
	CleanupConsumer(sarama.ConsumerGroupSession) error
	ConsumerError() error
}

// ConsumerStopSignal is an optional terminal-failure capability. A non-nil
// context must be stable for the handler lifetime. Its cancellation stops this
// endpoint's consume loop, including idle claims, without retrying a new group
// session. Normal session revocation must not cancel it. A nil context retains
// the default retry policy. Construct a new handler to recover from terminal
// failure; this signal never acknowledges or commits messages.
type ConsumerStopSignal interface {
	ConsumerStopContext() context.Context
}

func bindConsumerStop(lifecycle ConsumerLifecycle, cancel context.CancelFunc) func() bool {
	if policy, ok := lifecycle.(ConsumerStopSignal); ok {
		if terminal := policy.ConsumerStopContext(); terminal != nil {
			// Avoid starting another session if failure was already latched.
			if terminal.Err() != nil {
				cancel()
				return func() bool { return false }
			}
			return context.AfterFunc(terminal, cancel)
		}
	}
	return func() bool { return false }
}

func configureConsumerLifecycle(config *sarama.Config, lifecycle ConsumerLifecycle) error {
	if lifecycle == nil {
		return nil
	}
	if err := lifecycle.ConfigureConsumer(config); err != nil {
		return err
	}
	return config.Validate()
}
