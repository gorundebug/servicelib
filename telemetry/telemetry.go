/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package telemetry

import (
	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/telemetry/metrics"
	"github.com/gorundebug/servicelib/telemetry/prometeus"
	log "github.com/sirupsen/logrus"
)

func CreateMetrics(metricsEngine api.MetricsEngine, namespace string) metrics.Metrics {
	switch metricsEngine {
	case api.Prometeus:
		return &prometeus.Metrics{Namespace: namespace}
	}
	log.Fatalf("Unsupported metrics engine: %d", metricsEngine)
	return nil
}
