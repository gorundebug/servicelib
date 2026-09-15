package runtime

import (
	"context"
	"testing"

	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

type endpointAttributeStream struct {
	ServiceStream[int]
	reads int
}

func (s *endpointAttributeStream) GetName() string {
	s.reads++
	return "Reserve"
}

func (s *endpointAttributeStream) GetPipelineName() string {
	s.reads++
	return "booking"
}

func (s *endpointAttributeStream) GetComponentName() string {
	s.reads++
	return "Inventory"
}

type attributeEndpoint struct{ reads int }

func (ep *attributeEndpoint) GetName() string              { ep.reads++; return "Reserve Inventory" }
func (*attributeEndpoint) GetID() int                      { return 1 }
func (*attributeEndpoint) GetDataConnector() DataConnector { return nil }

func TestEndpointAttributesCaptureTypedLabelsOnce(t *testing.T) {
	stream := &endpointAttributeStream{}
	endpoint := &attributeEndpoint{}
	attributes := MakeEndpointSpanAttributes(stream, endpoint)
	want := map[string]string{
		"stream": "Reserve", "pipeline": "booking",
		"component": "Inventory", "endpoint": "Reserve Inventory",
	}
	for _, attribute := range attributes {
		if attribute.Value.AsString() != want[attribute.Key] {
			t.Fatalf("unexpected cached attribute: %s", attribute.Key)
		}
		delete(want, attribute.Key)
	}
	if len(want) != 0 {
		t.Fatalf("missing labels: %v", want)
	}
	if stream.reads != 3 || endpoint.reads != 1 {
		t.Fatalf("unexpected initialization reads: stream=%d endpoint=%d", stream.reads, endpoint.reads)
	}

	tracer := &groupingTracer{}
	ctx := tracing.EnableSampling(context.Background())
	allocations := testing.AllocsPerRun(1000, func() {
		_, span := tracer.Start(ctx, "temporal.input", attributes[:]...)
		span.End()
	})
	if allocations != 0 {
		t.Fatalf("cached attribute passing allocated %g objects", allocations)
	}
	if stream.reads != 3 || endpoint.reads != 1 {
		t.Fatal("reusing cached attributes consulted stream/endpoint getters")
	}
}
