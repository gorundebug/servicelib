/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package pool

import (
	"context"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/telemetry/metrics"
)

type Pool interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context)
}

func MakeDelayTaskPool(cfg config.ServiceEnvironmentConfig, m metrics.Metrics) DelayPool {
	return makeDelayPool(cfg, m)
}

func MakePriorityTaskPool(cfg config.ServiceEnvironmentConfig, name string, m metrics.Metrics) PriorityTaskPool {
	return makePriorityTaskPool(cfg, name, m)
}

func MakeTaskPool(cfg config.ServiceEnvironmentConfig, name string, m metrics.Metrics) TaskPool {
	return makeTaskPool(cfg, name, m)
}
