/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package logrus

import (
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/sirupsen/logrus"
	"sync"
)

var logsEngine *LogEngine
var once sync.Once

type LogEngine struct {
	environment environment.ServiceEnvironment
}

func (l *LogEngine) DefaultLogger(cfg *log.Config) log.Logger {
	return logrus.StandardLogger()
}

func CreateLogsEngine(env environment.ServiceEnvironment) log.LogsEngine {
	once.Do(func() {
		logsEngine = &LogEngine{environment: env}
	})
	return logsEngine
}
