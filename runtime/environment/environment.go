/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package environment

import (
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/httproute"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

type ServiceDependency interface {
	MetricsEngine(env ServiceEnvironment) metrics.MetricsEngine
	LogsEngine(env ServiceEnvironment) log.LogsEngine
	HttpRouter(env ServiceEnvironment) httproute.HttpRoute
}

type ServiceEnvironment interface {
	AppConfig() *config.ServiceAppConfig
	ServiceConfig() *config.ServiceConfig
	Metrics() metrics.Metrics
	Log() log.Logger
	ServiceDependency() ServiceDependency
	ServiceContext() interface{}
}
