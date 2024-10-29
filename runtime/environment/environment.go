/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package environment

import (
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

type ServiceDependency interface {
	GetMetricsEngine(env ServiceEnvironment) metrics.MetricsEngine
	GetLogsEngine(env ServiceEnvironment) log.LogsEngine
}

type ServiceEnvironment interface {
	GetAppConfig() *config.ServiceAppConfig
	GetServiceConfig() *config.ServiceConfig
	GetMetrics() metrics.Metrics
	GetLog() log.Logger
}
