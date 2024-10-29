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

type MetricsEngine int

const (
	Prometheus MetricsEngine = 1
)

func CreateMetricsEngine(metricsEngine MetricsEngine, env environment.ServiceEnvironment) (metrics.MetricsEngine, error) {
	switch metricsEngine {
	case Prometheus:
		return prometheus.CreateMetricsEngine(env), nil
	}
	return nil, fmt.Errorf("unsupported metrics engine: %d", metricsEngine)
}
