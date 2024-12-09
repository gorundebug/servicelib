/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package datasink

import (
	"github.com/gorundebug/servicelib/datasink/kafka"
	"github.com/gorundebug/servicelib/datasink/localsink"
	"github.com/gorundebug/servicelib/runtime"
)

func CustomEndpointSink[T, R any](stream runtime.TypedSinkStream[T, R], dataConsumer localsink.DataConsumer[T]) runtime.SinkConsumer[T] {
	return localsink.MakeCustomEndpointSink[T, R](stream, dataConsumer)
}

func SaramaKafkaEndpointSink[T, R any](stream runtime.TypedSinkStream[T, R], partitioner kafka.Partitioner[T]) runtime.SinkConsumer[T] {
	return kafka.MakeSaramaKafkaEndpointSink[T, R](stream, partitioner)
}
