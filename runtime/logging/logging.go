/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package logging

import (
	"fmt"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/logging/logrus"
)

type LogsEngine int

const (
	Logrus LogsEngine = 1
)

func CreateLogsEngine(logsEngine LogsEngine, env environment.ServiceEnvironment) (log.LogsEngine, error) {
	switch logsEngine {
	case Logrus:
		return logrus.CreateLogsEngine(env), nil
	}
	return nil, fmt.Errorf("unsupported logs engine: %d", logsEngine)
}
