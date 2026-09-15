package operators

import (
	"testing"

	"github.com/gorundebug/servicelib/runtime"
)

type groupedParentStream struct {
	runtime.ServiceStream[int]
}

func (*groupedParentStream) GetPipelineName() string  { return "reserve" }
func (*groupedParentStream) GetComponentName() string { return "Pricing" }

func TestStreamLinkPreservesParentGrouping(t *testing.T) {
	parent := &groupedParentStream{}
	link := &streamLink{stream: parent}
	var stream runtime.Stream = link
	pipeline, component := runtime.StreamGrouping(stream)
	if pipeline != "reserve" || component != "Pricing" {
		t.Fatalf("link lost parent grouping: %q / %q", pipeline, component)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		runtime.StreamGrouping(stream)
	}); allocations != 0 {
		t.Fatalf("link grouping allocated %g objects", allocations)
	}
}
