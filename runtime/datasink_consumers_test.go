package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type testOutputEndpointConsumer struct {
	name       string
	events     *[]string
	startError error
}

func (*testOutputEndpointConsumer) Endpoint() SinkEndpoint { return nil }

func (consumer *testOutputEndpointConsumer) Start(context.Context) error {
	*consumer.events = append(*consumer.events, "start:"+consumer.name)
	return consumer.startError
}

func (consumer *testOutputEndpointConsumer) Stop(context.Context) {
	*consumer.events = append(*consumer.events, "stop:"+consumer.name)
}

func TestDataSinkEndpointOwnsEveryConsumerLifecycle(t *testing.T) {
	events := make([]string, 0, 4)
	endpoint := &DataSinkEndpoint{}
	endpoint.AddEndpointConsumer(&testOutputEndpointConsumer{name: "first", events: &events})
	endpoint.AddEndpointConsumer(&testOutputEndpointConsumer{name: "second", events: &events})

	if err := endpoint.StartEndpointConsumers(context.Background()); err != nil {
		t.Fatal(err)
	}
	endpoint.StopEndpointConsumers(context.Background())

	expected := []string{"start:first", "start:second", "stop:second", "stop:first"}
	if !reflect.DeepEqual(events, expected) {
		t.Fatalf("events = %v, expected %v", events, expected)
	}
}

func TestDataSinkEndpointRollsBackStartedConsumers(t *testing.T) {
	events := make([]string, 0, 3)
	endpoint := &DataSinkEndpoint{}
	endpoint.AddEndpointConsumer(&testOutputEndpointConsumer{name: "first", events: &events})
	endpoint.AddEndpointConsumer(&testOutputEndpointConsumer{
		name: "second", events: &events, startError: errors.New("start failed"),
	})

	if err := endpoint.StartEndpointConsumers(context.Background()); err == nil {
		t.Fatal("expected start failure")
	}
	expected := []string{"start:first", "start:second", "stop:first"}
	if !reflect.DeepEqual(events, expected) {
		t.Fatalf("events = %v, expected %v", events, expected)
	}
}

func TestDataSinkEndpointRollsBackWhenConsumerHasNoLifecycle(t *testing.T) {
	events := make([]string, 0)
	endpoint := &DataSinkEndpoint{}
	endpoint.AddEndpointConsumer(&testOutputEndpointConsumer{name: "first", events: &events})
	endpoint.AddEndpointConsumer(testOutputEndpointWithoutLifecycle{})

	if err := endpoint.StartEndpointConsumers(context.Background()); err == nil {
		t.Fatal("expected missing lifecycle error")
	}

	want := []string{"start:first", "stop:first"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("unexpected lifecycle events: got %v, want %v", events, want)
	}
}

type testOutputEndpointWithoutLifecycle struct{}

func (testOutputEndpointWithoutLifecycle) Endpoint() SinkEndpoint { return nil }
