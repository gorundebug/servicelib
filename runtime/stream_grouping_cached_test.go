package runtime

import "testing"

func TestTransportGroupingUsesCachedStreamFieldsWithoutAllocation(t *testing.T) {
	// No environment/config registry exists: the accessor must use only fields
	// that were captured when the concrete stream was constructed.
	stream := &ServiceStream[int]{grouping: streamGrouping{
		pipeline: "pricing", component: "Customer Pricing",
	}}
	pipeline, component := StreamGrouping(stream)
	if pipeline != "pricing" || component != "Customer Pricing" {
		t.Fatalf("unexpected grouping: %q / %q", pipeline, component)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		StreamGrouping(stream)
	}); allocations != 0 {
		t.Fatalf("cached label lookup allocated %g objects", allocations)
	}
	if pipeline, component := StreamGrouping(&ServiceStream[int]{}); pipeline != "" || component != "" {
		t.Fatal("synthetic streams must not inherit another stream's grouping")
	}
}
