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
	index    int
}

type PriorityTaskPool interface {
	Pool
	Execute(fn func(), priority int) *PriorityTask
}

type TaskPriorityQueue []*PriorityTask

func (pq *TaskPriorityQueue) Len() int { return len(*pq) }

func (pq *TaskPriorityQueue) Less(i, j int) bool {
	return (*pq)[i].priority < (*pq)[j].priority
}

func (pq *TaskPriorityQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
	(*pq)[i].index = i
	(*pq)[j].index = j
}

func (pq *TaskPriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*PriorityTask)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *TaskPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}
