package kafka

import (
	"context"
	"github.com/IBM/sarama"
)

type lifecycleSession struct {
	sarama.ConsumerGroupSession
	ctx context.Context
}

func (s *lifecycleSession) Context() context.Context { return s.ctx }

type lifecycleClaim struct {
	sarama.ConsumerGroupClaim
	messages chan *sarama.ConsumerMessage
}

func (c lifecycleClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }
