/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package store

import "context"

type Storage interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context)
}

func MakeDelayTaskPool(executorsCount int) DelayPool {
	return makeDelayPool(executorsCount)
}
