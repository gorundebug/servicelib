/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package pool

import "sync"

type Task struct {
	fn   func()
	next *Task
	prev *Task
}

type TaskPool interface {
	Pool
	AddTask(fn func()) *Task
}

type TaskPoolImpl struct {
	head *Task
	tail *Task
	lock sync.Mutex
	name string
}

func (p *TaskPoolImpl) AddTask(fn func()) *Task {
	p.lock.Lock()
	defer p.lock.Unlock()
	task := &Task{fn: fn}
	if p.tail != nil {
		task.prev = p.tail
		p.tail.next = task
	} else {
		p.head = task
	}
	p.tail = task
	return task
}
