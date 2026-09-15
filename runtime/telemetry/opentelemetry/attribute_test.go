package opentelemetry

import (
	"reflect"
	"testing"

	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"go.opentelemetry.io/otel/attribute"
)

var convertedAttribute attribute.KeyValue

func TestTypedAttributeConversion(t *testing.T) {
	input := []tracing.Attribute{tracing.StringAttr("s", "booking"), tracing.Int64Attr("i", -1234567), tracing.Float64Attr("f", 12.5), tracing.BoolAttr("b", true), {Key: "invalid"}}
	want := []attribute.KeyValue{attribute.String("s", "booking"), attribute.Int64("i", -1234567), attribute.Float64("f", 12.5), attribute.Bool("b", true), {Key: "invalid"}}
	if got := attrsFromTracing(input); !reflect.DeepEqual(got, want) {
		t.Fatalf("conversion: got %v, want %v", got, want)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		for _, attr := range input {
			convertedAttribute = attrFromTracing(attr)
		}
	}); allocations != 0 {
		t.Fatalf("scalar conversion allocated: %g", allocations)
	}
}
