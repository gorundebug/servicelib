/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package store

import (
	"context"
	"time"
)

type DelayPool interface {
	Delay(deadline time.Duration, fn func()) *DelayTask
	Start(ctx context.Context)
	Stop(ctx context.Context)
}

func MakeDelayPool(runnersCount int) DelayPool {
	return &DelayPoolImpl{
		runnersCount: runnersCount,
	}
}

type DelayTask struct {
	deadline time.Time
	fn       func()
	index    int
}

type PriorityQueue []*DelayTask

func (pq *PriorityQueue) Len() int { return len(*pq) }

func (pq *PriorityQueue) Less(i, j int) bool {
	return (*pq)[i].deadline.Before((*pq)[j].deadline)
}

func (pq *PriorityQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
	(*pq)[i].index = i
	(*pq)[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*DelayTask)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) down(i, n int) {
	for {
		left := 2*i + 1
		right := 2*i + 2
		smallest := i
		if left < n && pq.Less(left, smallest) {
			smallest = left
		}
		if right < n && pq.Less(right, smallest) {
			smallest = right
		}
		if smallest == i {
			break
		}
		pq.Swap(i, smallest)
		i = smallest
	}
}

func (pq *PriorityQueue) up(i int) {
	for {
		parent := (i - 1) / 2
		if i == 0 || pq.Less(parent, i) {
			break
		}
		pq.Swap(i, parent)
		i = parent
	}
}

func (pq *PriorityQueue) Remove(item *DelayTask) {
	index := item.index
	n := len(*pq)
	pq.Swap(index, n-1)
	*pq = (*pq)[:n-1]
	pq.down(index, n-1)
	pq.up(index)
	item.index = -1
}

type DelayPoolImpl struct {
	runnersCount int
}

func (p *DelayPoolImpl) Delay(deadline time.Duration, fn func()) *DelayTask {
	return nil
}

func (p *DelayPoolImpl) Start(ctx context.Context) {
}

func (p *DelayPoolImpl) Stop(ctx context.Context) {
}
