/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package saruntime

import (
	"reflect"
)

type StreamExecutionEnvironment interface {
	GetSerde(valueType reflect.Type) (Serializer, error)
	GetConfig() *ServiceAppConfig
	ServiceInit(config Config)
	ConfigReload(config Config)
	Start() error
	Stop()
}
type Caller[T any] interface {
	Consume(value T)
}
