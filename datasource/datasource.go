/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package datasource

import (
	"github.com/gorundebug/servicelib/datasource/http"
	"github.com/gorundebug/servicelib/datasource/localsource"
	"github.com/gorundebug/servicelib/runtime"
)

func CustomEndpointConsumer[T any](stream runtime.TypedInputStream[T], dataProducer localsource.DataProducer[T]) runtime.Consumer[T] {
	return localsource.MakeCustomEndpointConsumer(stream, dataProducer)
}

func NetHTTPEndpointConsumer[T any](stream runtime.TypedInputStream[T]) runtime.Consumer[T] {
	return http.MakeNetHTTPEndpointConsumer(stream)
}
