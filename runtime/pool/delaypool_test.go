/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package pool

import (
	"container/heap"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_PriorityQueue(t *testing.T) {
	pq := &PriorityQueue{}
	heap.Push(pq, &DelayTask{deadline: time.Unix(1, 0)})
	heap.Push(pq, &DelayTask{deadline: time.Unix(7, 0)})
	heap.Push(pq, &DelayTask{deadline: time.Unix(20, 0)})
	heap.Push(pq, &DelayTask{deadline: time.Unix(4, 0)})
	task := &DelayTask{deadline: time.Unix(19, 0)}
	heap.Push(pq, task)
	heap.Push(pq, &DelayTask{deadline: time.Unix(4, 0)})
	heap.Push(pq, &DelayTask{deadline: time.Unix(12, 0)})
	heap.Remove(pq, task.index)
	assert.Equal(t, time.Unix(1, 0), heap.Pop(pq).(*DelayTask).deadline)
	assert.Equal(t, time.Unix(4, 0), heap.Pop(pq).(*DelayTask).deadline)
	assert.Equal(t, time.Unix(4, 0), heap.Pop(pq).(*DelayTask).deadline)
	assert.Equal(t, time.Unix(7, 0), heap.Pop(pq).(*DelayTask).deadline)
	assert.Equal(t, time.Unix(12, 0), heap.Pop(pq).(*DelayTask).deadline)
	assert.Equal(t, time.Unix(20, 0), heap.Pop(pq).(*DelayTask).deadline)
	assert.Equal(t, 0, pq.Len())
}
