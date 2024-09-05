/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package pool

type Task struct {
	fn func()
}

type TaskPool interface {
	Pool
	Execute(fn func()) *Task
}
