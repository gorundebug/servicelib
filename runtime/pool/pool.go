/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package pool

import (
	"context"
	"github.com/gorundebug/servicelib/runtime/environment"
)

type Pool interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context)
}

func MakeDelayTaskPool(env environment.ServiceEnvironment) DelayPool {
	return makeDelayPool(env)
}

func MakePriorityTaskPool(env environment.ServiceEnvironment, name string) PriorityTaskPool {
	return makePriorityTaskPool(env, name)
}

func MakeTaskPool(env environment.ServiceEnvironment, name string) TaskPool {
	return makeTaskPool(env, name)
}
