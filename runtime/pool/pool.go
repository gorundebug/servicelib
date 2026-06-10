/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package pool

import (
	"context"
	"errors"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
)

var (
	ErrPoolStopped        = errors.New("pool stopped")
	ErrPoolAlreadyStarted = errors.New("pool already started")
)

type Pool interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context)
}

func runTask(ctx context.Context, env environment.ServiceEnvironment, poolName string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			env.Log().Error(ctx, "panic in pool worker", log.Str("pool", poolName), log.Any("panic", r))
			panic(r)
		}
	}()
	fn()
}

func MakeDelayTaskPool(env environment.ServiceEnvironment) (DelayPool, error) {
	return makeDelayPool(env)
}

func MakeGoroutineDelayTaskPool(env environment.ServiceEnvironment) (DelayPool, error) {
	return makeGoroutineDelayPool(env)
}

func MakePriorityTaskPool(env environment.ServiceEnvironment, poolConfig *config.PoolConfig) (PriorityTaskPool, error) {
	return makePriorityTaskPool(env, poolConfig)
}

func MakeTaskPool(env environment.ServiceEnvironment, poolConfig *config.PoolConfig) (TaskPool, error) {
	return makeTaskPool(env, poolConfig)
}
