package kafka

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/IBM/sarama"
)

// Match Sarama CreateTopic's controller retry policy, without its unconditional
// sleep. Do not retry authorization, validation or other broker errors.
func retryStartupTopic(ctx context.Context, retries int, backoff time.Duration, create func() error) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := create()
		if cancelled := ctx.Err(); cancelled != nil {
			return errors.Join(err, cancelled)
		}
		if err == nil || attempt >= retries || !(errors.Is(err, sarama.ErrNotController) || errors.Is(err, io.EOF)) {
			return err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, ctx.Err())
		case <-timer.C:
		}
	}
}
