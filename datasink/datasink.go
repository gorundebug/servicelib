/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package datasink

import (
	"gitlab.com/gorundebug/servicelib/datasink/localsink"
	"gitlab.com/gorundebug/servicelib/runtime"
)

func CustomEndpointSink[T any](stream runtime.TypedSinkStream[T], dataConsumer localsink.DataConsumer[T]) runtime.Consumer[T] {
	return CustomEndpointSink[T](stream, dataConsumer)
}
