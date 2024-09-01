/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package datasink

import (
	"github.com/gorundebug/servicelib/datasink/localsink"
	"github.com/gorundebug/servicelib/runtime"
)

func CustomEndpointSink[T any](stream runtime.TypedSinkStream[T], dataConsumer localsink.DataConsumer[T]) runtime.Consumer[T] {
	return localsink.MakeCustomEndpointSink[T](stream, dataConsumer)
}
