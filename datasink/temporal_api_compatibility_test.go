package datasink_test

import (
	"github.com/gorundebug/servicelib/datasink"
	temporal "github.com/gorundebug/servicelib/datasink/temporal"
	"github.com/gorundebug/servicelib/runtime"
)

// These assignments protect function-value compatibility, not just ordinary
// calls. Appending even an optional variadic parameter would break this API.
var (
	_ func(runtime.TypedSinkStream[int, error]) (runtime.Consumer[int], error)                                                          = datasink.TemporalEndpointConsumer[int, error]
	_ func(runtime.TypedSinkStream[int, error], temporal.EndpointHandler[string, int]) (runtime.Consumer[int], error)                   = datasink.TemporalEndpointConsumerWithHandler[string, int, error]
	_ func(runtime.TypedSinkStreamWithResult[int, string, error]) (runtime.Consumer[int], error)                                        = datasink.TemporalEndpointConsumerWithResult[int, string, error]
	_ func(runtime.TypedSinkStreamWithResult[int, string, error], temporal.EndpointHandler[string, int]) (runtime.Consumer[int], error) = datasink.TemporalEndpointConsumerWithResultAndHandler[string, int, string, error]
	_ func(runtime.TypedSinkStream[int, error]) (runtime.Consumer[int], error)                                                          = temporal.MakeDirectEndpointConsumer[int, error]
	_ func(runtime.TypedSinkStream[int, error], temporal.EndpointHandler[string, int]) (runtime.Consumer[int], error)                   = temporal.MakeEndpointConsumer[string, int, error]
	_ func(runtime.TypedSinkStreamWithResult[int, string, error]) (runtime.Consumer[int], error)                                        = temporal.MakeDirectEndpointConsumerWithResult[int, string, error]
	_ func(runtime.TypedSinkStreamWithResult[int, string, error], temporal.EndpointHandler[string, int]) (runtime.Consumer[int], error) = temporal.MakeEndpointConsumerWithResult[string, int, string, error]
)
