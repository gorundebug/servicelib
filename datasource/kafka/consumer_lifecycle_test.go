package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
)

type lifecycleFixture struct {
	configure      func(*sarama.Config) error
	setup, cleanup func(sarama.ConsumerGroupSession) error
	failure        error
}

func (f *lifecycleFixture) ConfigureConsumer(c *sarama.Config) error {
	if f.configure != nil {
		return f.configure(c)
	}
	return nil
}
func (f *lifecycleFixture) SetupConsumer(s sarama.ConsumerGroupSession) error {
	if f.setup != nil {
		return f.setup(s)
	}
	return nil
}
func (f *lifecycleFixture) CleanupConsumer(s sarama.ConsumerGroupSession) error {
	if f.cleanup != nil {
		return f.cleanup(s)
	}
	return nil
}
func (f *lifecycleFixture) ConsumerError() error { return f.failure }

type lifecycleSession struct {
	sarama.ConsumerGroupSession
	ctx   context.Context
	reset int64
}

func (s *lifecycleSession) Context() context.Context { return s.ctx }
func (s *lifecycleSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	s.reset = offset
}

type lifecycleClaim struct {
	sarama.ConsumerGroupClaim
	messages chan *sarama.ConsumerMessage
}

func (c lifecycleClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

type lifecycleConsumer struct {
	saramaKafkaEndpointConsumer
	request func(*sarama.ConsumerMessage)
}

func (c lifecycleConsumer) EndpointRequest(_ sarama.ConsumerGroupSession, _ sarama.ConsumerGroupClaim, m *sarama.ConsumerMessage) {
	c.request(m)
}

func TestConsumerLifecycleConfigurationAndAssignment(t *testing.T) {
	want := errors.New("recovery failed")
	config := sarama.NewConfig()
	hooks := &lifecycleFixture{configure: func(c *sarama.Config) error {
		c.Consumer.IsolationLevel = sarama.ReadCommitted
		c.Consumer.Offsets.AutoCommit.Enable = false
		return nil
	}}
	if err := configureConsumerLifecycle(config, hooks); err != nil {
		t.Fatal(err)
	}
	if config.Consumer.IsolationLevel != sarama.ReadCommitted || config.Consumer.Offsets.AutoCommit.Enable {
		t.Fatal("consumer configuration ignored")
	}
	session := &lifecycleSession{ctx: context.Background()}
	hooks.setup = func(s sarama.ConsumerGroupSession) error { s.ResetOffset("catalog", 0, 42, ""); return nil }
	hooks.cleanup = func(s sarama.ConsumerGroupSession) error {
		if s != session {
			t.Fatal("wrong session")
		}
		return want
	}
	endpoint := &saramaKafkaEndpoint{lifecycle: hooks}
	if err := endpoint.Setup(session); err != nil || session.reset != 42 {
		t.Fatal("setup", err, session.reset)
	}
	if err := endpoint.Cleanup(session); !errors.Is(err, want) {
		t.Fatal("cleanup", err)
	}
	hooks.setup = func(sarama.ConsumerGroupSession) error { return want }
	if err := endpoint.Setup(session); !errors.Is(err, want) {
		t.Fatal("assignment failure swallowed", err)
	}
	hooks.configure = func(*sarama.Config) error { return want }
	if err := configureConsumerLifecycle(config, hooks); !errors.Is(err, want) {
		t.Fatal(err)
	}
	hooks.configure = func(c *sarama.Config) error { c.Net.ReadTimeout = 0; return nil }
	if err := configureConsumerLifecycle(config, hooks); err == nil {
		t.Fatal("invalid override accepted")
	}
}

func TestConsumerLifecycleStopsClaimAfterFailedMessage(t *testing.T) {
	want := errors.New("catalog update failed")
	hooks := &lifecycleFixture{}
	session := &lifecycleSession{ctx: context.Background()}
	messages := make(chan *sarama.ConsumerMessage, 2)
	messages <- &sarama.ConsumerMessage{Offset: 42}
	messages <- &sarama.ConsumerMessage{Offset: 43}
	close(messages)
	called := 0
	endpoint := &saramaKafkaEndpoint{lifecycle: hooks, consumer: lifecycleConsumer{request: func(m *sarama.ConsumerMessage) {
		called++
		if m.Offset != 42 {
			t.Fatal("skipped failed update")
		}
		hooks.failure = want
	}}}
	if err := endpoint.ConsumeClaim(session, lifecycleClaim{messages: messages}); !errors.Is(err, want) || called != 1 {
		t.Fatal(called, err)
	}
	if err := endpoint.ConsumeClaim(session, lifecycleClaim{messages: messages}); !errors.Is(err, want) || called != 1 {
		t.Fatal("failure was not latched", called, err)
	}
}

func TestConsumerLifecycleDefaultAndCancellation(t *testing.T) {
	config := sarama.NewConfig()
	if err := configureConsumerLifecycle(config, nil); err != nil || config.Consumer.IsolationLevel != sarama.ReadUncommitted || !config.Consumer.Offsets.AutoCommit.Enable {
		t.Fatal("default changed", err)
	}
	endpoint := &saramaKafkaEndpoint{}
	session := &lifecycleSession{ctx: context.Background()}
	if err := endpoint.Setup(session); err != nil {
		t.Fatal(err)
	}
	if err := endpoint.Cleanup(session); err != nil {
		t.Fatal(err)
	}
	messages := make(chan *sarama.ConsumerMessage, 2)
	messages <- &sarama.ConsumerMessage{}
	messages <- &sarama.ConsumerMessage{}
	close(messages)
	called := 0
	endpoint.consumer = lifecycleConsumer{request: func(*sarama.ConsumerMessage) { called++ }}
	if err := endpoint.ConsumeClaim(session, lifecycleClaim{messages: messages}); err != nil || called != 2 {
		t.Fatal("default consumption", called, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	session.ctx = ctx
	if err := endpoint.ConsumeClaim(session, lifecycleClaim{messages: make(chan *sarama.ConsumerMessage)}); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled claim blocked", err)
	}
}
