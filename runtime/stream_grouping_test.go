package runtime

import (
	"context"
	"reflect"
	"testing"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
)

type groupingTracer struct{ attributes []tracing.Attribute }

func (tr *groupingTracer) Start(ctx context.Context, _ string, attributes ...tracing.Attribute) (context.Context, tracing.Span) {
	tr.attributes = attributes
	return ctx, noopSpan{}
}
func groupingAttributes(attributes []tracing.Attribute) map[string]string {
	result := make(map[string]string, len(attributes))
	for _, attribute := range attributes {
		result[attribute.Key] = attribute.Value.AsString()
	}
	return result
}

func TestStreamGroupingMetadata(t *testing.T) {
	cfg := &config.MapStreamConfig{Pipeline: "reserve", Component: "Pricing"}
	if got := groupingForStream(cfg); got != (streamGrouping{pipeline: "reserve", component: "Pricing"}) {
		t.Fatalf("unexpected grouping: %+v", got)
	}
	if got := groupingForStream(nil); got != (streamGrouping{}) {
		t.Fatalf("nil config: %+v", got)
	}
}

func TestOperatorSpanUsesOnlyPipelineAndComponentDefinitionNames(t *testing.T) {
	tracer := &groupingTracer{}
	stream := ServiceStream[int]{name: "CalculatePrice", grouping: streamGrouping{pipeline: "reserve", component: "Pricing"}, tracer: tracer}
	ctx := tracing.EnableSampling(context.Background())
	_, span := stream.StartSpan(ctx, "stream.map")
	span.End()
	want := map[string]string{"stream": "CalculatePrice", "pipeline": "reserve", "component": "Pricing"}
	if got := groupingAttributes(tracer.attributes); !reflect.DeepEqual(got, want) {
		t.Fatalf("attributes: %v", got)
	}
	tracer.attributes = nil
	stream.StartSpan(context.Background(), "stream.map")
	if tracer.attributes != nil {
		t.Fatal("unsampled operation created a span")
	}
}

func TestLinkSpanUsesReceivingGroupingWithoutOccurrenceIdentity(t *testing.T) {
	tracer := &groupingTracer{}
	c := directCaller[int]{caller: caller[int]{fromName: "Request", toName: "LoadCustomer", grouping: streamGrouping{pipeline: "reserve", component: "Pricing"}, tracer: tracer}}
	_, span := c.startSpan(tracing.EnableSampling(context.Background()))
	span.End()
	want := map[string]string{"from": "Request", "to": "LoadCustomer", "pipeline": "reserve", "component": "Pricing"}
	if got := groupingAttributes(tracer.attributes); !reflect.DeepEqual(got, want) {
		t.Fatalf("attributes: %v", got)
	}
}
