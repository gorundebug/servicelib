/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package pool

type PriorityTask struct {
	fn       func()
	priority int
}

type PriorityTaskPool interface {
	Pool
	Execute(fn func()) *PriorityTask
}
