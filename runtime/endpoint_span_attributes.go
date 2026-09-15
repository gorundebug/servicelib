package runtime

import "github.com/gorundebug/servicelib/runtime/environment/tracing"

// MakeEndpointSpanAttributes captures static endpoint labels during consumer
// construction. Call only when tracing is configured and reuse the returned
// array for sampled requests; sampling itself must still be checked per call.
func MakeEndpointSpanAttributes(stream Stream, endpoint Endpoint) [4]tracing.Attribute {
	return [4]tracing.Attribute{
		tracing.StringAttr("stream", stream.GetName()),
		tracing.StringAttr("pipeline", stream.GetPipelineName()),
		tracing.StringAttr("component", stream.GetComponentName()),
		tracing.StringAttr("endpoint", endpoint.GetName()),
	}
}
