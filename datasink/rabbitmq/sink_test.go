package rabbitmq

import (
	"errors"
	"github.com/Azure/go-amqp"
	"github.com/gorundebug/servicelib/internal/rabbitmqclient"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"testing"
)

func TestNativeSnapshotOwnsCallerPayloadAndProperties(t *testing.T) {
	ep := &endpoint{}
	m := rmq.NewMessage([]byte("body"))
	m.ApplicationProperties = map[string]any{"tenant": "first"}
	m.Properties = &amqp.MessageProperties{CorrelationID: "original"}
	copy, _, err := ep.snapshot(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	m.Data[0][0] = 'X'
	m.ApplicationProperties["tenant"] = "second"
	m.Properties.CorrelationID = "changed"
	if string(copy.GetData()) != "body" || copy.ApplicationProperties["tenant"] != "first" || copy.Properties.CorrelationID != "original" {
		t.Fatal("snapshot aliases caller")
	}
	_, _, err = ep.snapshot(t.Context(), rmq.NewMessage(make([]byte, rabbitmqclient.MaxPendingBytes+1)))
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("unbounded payload: %v", err)
	}
}
func TestTransportFailureIsUnknown(t *testing.T) {
	err := errors.New("connection lost")
	d, e := delivery(nil, err)
	if d.Outcome != Unknown || !errors.Is(e, err) {
		t.Fatalf("%+v %v", d, e)
	}
	d, e = delivery(&rmq.PublishResult{Outcome: &rmq.StateModified{}}, nil)
	if d.Outcome != Modified || e != nil {
		t.Fatalf("%+v %v", d, e)
	}
}
