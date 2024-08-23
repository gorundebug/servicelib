/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package telemetry

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/gorundebug/servicelib/api"
	"gitlab.com/gorundebug/servicelib/telemetry/metrics"
	"gitlab.com/gorundebug/servicelib/telemetry/prometeus"
)

func CreateMetrics(metricsEngine api.MetricsEngine) metrics.Metrics {
	switch metricsEngine {
	case api.Prometeus:
		return &prometeus.Metrics{}
	}
	log.Fatalf("Unsupported metrics engine: %d", metricsEngine)
	return nil
}
