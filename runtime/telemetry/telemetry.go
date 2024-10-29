/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package telemetry

import (
	"fmt"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/telemetry/prometheus"
)

type MetricsEngineType int

const (
	Prometheus MetricsEngineType = 1
)

func CreateMetricsEngine(metricsEngineType MetricsEngineType, env environment.ServiceEnvironment) (metrics.MetricsEngine, error) {
	switch metricsEngineType {
	case Prometheus:
		return prometheus.CreateMetricsEngine(env), nil
	}
	return nil, fmt.Errorf("unsupported metrics engine type: %d", metricsEngineType)
}
