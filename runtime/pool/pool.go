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
	"github.com/gorundebug/servicelib/runtime/environment"
)

type Pool interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context)
}

func runTask(env environment.ServiceEnvironment, poolName string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			env.Log().Errorf("panic in pool %q worker: %v", poolName, r)
			panic(r)
		}
	}()
	fn()
}

func MakeDelayTaskPool(env environment.ServiceEnvironment) (DelayPool, error) {
	return makeDelayPool(env)
}

func MakePriorityTaskPool(env environment.ServiceEnvironment, poolConfig *config.PoolConfig) (PriorityTaskPool, error) {
	return makePriorityTaskPool(env, poolConfig)
}

func MakeTaskPool(env environment.ServiceEnvironment, poolConfig *config.PoolConfig) (TaskPool, error) {
	return makeTaskPool(env, poolConfig)
}
