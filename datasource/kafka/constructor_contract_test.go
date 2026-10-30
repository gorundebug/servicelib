package kafka

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"gopkg.in/yaml.v3"
)

// An application method with this name must not silently extend the SDK contract.
type ordinaryConstructorHandler struct{ *requestLifecycleHandler }

func (*ordinaryConstructorHandler) SetupConsumer(sarama.ConsumerGroupSession) error {
	panic("hidden application setup invoked")
}
func (*ordinaryConstructorHandler) CleanupConsumer(sarama.ConsumerGroupSession) error {
	panic("hidden application cleanup invoked")
}
func (*ordinaryConstructorHandler) ConsumerError() error {
	panic("hidden application failure policy invoked")
}

func (h *ordinaryConstructorHandler) ConsumeMessage(_ context.Context, _ StreamContext[int, int, error], _ int, _ ConsumerMessage, _ ResultContext[int, int, int, error]) error {
	h.consumed.Add(1)
	return nil
}

func TestOrdinaryKafkaConstructorKeepsApplicationPropertiesOpaque(t *testing.T) {
	var endpoint config.KafkaEndpointConfig
	// The SDK must not interpret or validate settings owned by the application.
	if err := yaml.Unmarshal([]byte("id: 2\nname: events\nidDataConnector: 1\nbatchMaxRecords: application-defined\nbatchMaxWaitMs: 50\nexternalOffsets: true\n"), &endpoint); err != nil {
		t.Fatal(err)
	}
	if endpoint.GetProperty("batchMaxRecords") != "application-defined" || endpoint.GetProperty("batchMaxWaitMs") != 50 || endpoint.GetProperty("externalOffsets") != true {
		t.Fatal("application properties were consumed by SDK fields", endpoint.Properties)
	}
	cfg, err := config.NewRuntimeConfig(constructorConfig{&endpoint})
	if err != nil {
		t.Fatal(err)
	}
	h := &ordinaryConstructorHandler{&requestLifecycleHandler{started: make(chan context.Context, 2), ended: make(chan error, 2)}}
	stream := constructorStream{env: &constructorEnvironment{cfg: cfg}}
	c, err := MakeSaramaKafkaEndpointConsumer[int, int, int, error](stream, h)
	if err != nil {
		t.Fatal(err)
	}
	consumer := c.(*saramaKafkaTypedEndpointConsumer[int, int, int, error])
	ep := c.(runtime.InputEndpointConsumer).Endpoint().(*saramaKafkaEndpoint)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := consumer.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer consumer.Stop(ctx)
	session := &requestLifecycleSession{lifecycleSession: lifecycleSession{ctx: ctx}}
	if err := ep.Setup(session); err != nil {
		t.Fatal(err)
	}
	messages := make(chan *sarama.ConsumerMessage, 2)
	// Completed user calls do not acknowledge either record.
	for offset := range int64(2) {
		messages <- &sarama.ConsumerMessage{Topic: "events", Partition: 1, Offset: offset}
	}
	close(messages)
	if err := ep.ConsumeClaim(session, lifecycleClaim{messages: messages}); err != nil {
		t.Fatal(err)
	}
	if err := ep.Cleanup(session); err != nil {
		t.Fatal(err)
	}
	if h.consumed.Load() != 2 || h.end.Load() != 2 {
		t.Fatal("ordinary requests blocked or changed their contract")
	}
	if session.marks.Load() != 0 || session.commits.Load() != 0 {
		t.Fatal("SDK acknowledged input implicitly")
	}
}
