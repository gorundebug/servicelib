/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package pool

import (
    "container/heap"
    "context"
    "sync"
    "time"

    "github.com/gorundebug/servicelib/runtime/environment"
    "github.com/gorundebug/servicelib/runtime/environment/log"
    "github.com/gorundebug/servicelib/runtime/environment/metrics"
)

type DelayPool interface {
    Pool
    Delay(ctx context.Context, deadline time.Duration, fn func()) error
}

type DelayTask struct {
    deadline time.Time
    fn       func()
    index    int
    stopFn   func() bool
}

type DelayTaskPriorityQueue []*DelayTask

func (pq *DelayTaskPriorityQueue) Len() int { return len(*pq) }

func (pq *DelayTaskPriorityQueue) Less(i, j int) bool {
    return (*pq)[i].deadline.Before((*pq)[j].deadline)
}

func (pq *DelayTaskPriorityQueue) Swap(i, j int) {
    (*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
    (*pq)[i].index = i
    (*pq)[j].index = j
}

func (pq *DelayTaskPriorityQueue) Push(x interface{}) {
    n := len(*pq)
    item := x.(*DelayTask)
    item.index = n
    *pq = append(*pq, item)
}

func (pq *DelayTaskPriorityQueue) Pop() interface{} {
    old := *pq
    n := len(old)
    item := old[n-1]
    old[n-1] = nil
    item.index = -1
    *pq = old[0 : n-1]
    return item
}

// DelayPoolImpl schedules tasks for deferred execution using a min-heap and a single
// shared timer. Unlike GoroutineDelayPool, it holds zero goroutines while tasks
// are waiting — only one goroutine (processTimer) wakes when the nearest deadline
// fires. This keeps the goroutine profile clean regardless of how many tasks are
// pending, which matters when tens of thousands of delayed tasks are in flight
// simultaneously and the extra goroutines would be visible in profiles or dashboards.
//
// Trade-off: all task dispatch is serialised through processTimer, so raw throughput
// and burst latency are lower than GoroutineDelayPool. Prefer GoroutineDelayPool
// for moderate concurrency (up to a few thousand pending tasks); use DelayPool when
// goroutine count is a hard constraint.
type DelayPoolImpl struct {
    pq                   *DelayTaskPriorityQueue
    wg                   sync.WaitGroup
    timer                *time.Timer
    lock                 sync.Mutex
    done                 bool
    stopOnce             sync.Once
    startOnce            sync.Once
    ctx                  context.Context
    gaugeWaitQueueLength metrics.Int64Gauge
    tasksTotal           metrics.Int64Counter
    executionDuration    metrics.Float64Histogram
    stopTimeoutCounter   metrics.Int64Counter
    taskCancelledCounter metrics.Int64Counter
    environment          environment.ServiceEnvironment
}

func makeDelayPool(env environment.ServiceEnvironment) (DelayPool, error) {
    pool := &DelayPoolImpl{
        pq:          &DelayTaskPriorityQueue{},
        environment: env,
    }
    scope := env.Metrics().Scope("delay_pool", metrics.Labels{
        "service": env.ServiceConfig().Name,
    })
    var err error
    pool.gaugeWaitQueueLength, err = scope.Gauge("wait_queue_length", "Delay pool wait queue length", nil)
    if err != nil {
        return nil, err
    }
    pool.tasksTotal, err = scope.Counter("tasks_total", "Total number of tasks executed by delay pool", nil)
    if err != nil {
        return nil, err
    }
    pool.executionDuration, err = scope.Histogram("task_execution_duration_seconds", "Task execution duration in seconds", nil)
    if err != nil {
        return nil, err
    }
    pool.stopTimeoutCounter, err = scope.Counter("events_total", "Total number of events in delay pool", metrics.Labels{"event": "stop_timeout"})
    if err != nil {
        return nil, err
    }
    pool.taskCancelledCounter, err = scope.Counter("events_total", "Total number of events in delay pool", metrics.Labels{"event": "task_cancelled"})
    if err != nil {
        return nil, err
    }
    return pool, nil
}

const processTimerBatch = 100

func (p *DelayPoolImpl) processTimer() {
    ctx := p.ctx
    var batch [processTimerBatch]*DelayTask
    for {
        n := 0
        p.lock.Lock()
        now := time.Now()
        moreExpired := true
        for n < processTimerBatch {
            moreExpired = p.pq.Len() > 0 && !(*p.pq)[0].deadline.After(now)
            if !moreExpired {
                break
            }
            task := heap.Pop(p.pq).(*DelayTask)
            task.stopFn()
            p.gaugeWaitQueueLength.Dec()
            batch[n] = task
            n++
        }
        if !moreExpired && p.pq.Len() > 0 {
            p.timer.Reset(time.Until((*p.pq)[0].deadline))
        }
        p.lock.Unlock()

        for i := range batch[:n] {
            task := batch[i]
            batch[i] = nil
            go func() {
                defer p.wg.Done()
                startTime := time.Now()
                runTask(ctx, p.environment, "delay", task.fn)
                task.fn = nil
                p.tasksTotal.Inc(ctx)
                p.executionDuration.Observe(ctx, time.Since(startTime).Seconds())
            }()
        }

        if !moreExpired {
            return
        }
    }
}

func (p *DelayPoolImpl) Delay(ctx context.Context, deadline time.Duration, fn func()) error {
    if err := ctx.Err(); err != nil {
        return err
    }
    if ctxDeadline, ok := ctx.Deadline(); ok {
        if remaining := time.Until(ctxDeadline); remaining < deadline {
            deadline = remaining
        }
    }
    task := &DelayTask{
        fn:    fn,
        index: -1,
    }
    p.lock.Lock()
    if p.done {
        p.lock.Unlock()
        return ErrPoolStopped
    }
    task.deadline = time.Now().Add(deadline)
    if p.pq.Len() == 0 || task.deadline.Before((*p.pq)[0].deadline) {
        if p.timer != nil {
            p.timer.Reset(deadline)
        } else {
            p.timer = time.AfterFunc(deadline, p.processTimer)
        }
    }
    heap.Push(p.pq, task)
    p.wg.Add(1)
    task.stopFn = context.AfterFunc(ctx, func() {
        p.lock.Lock()
        if task.index < 0 {
            p.lock.Unlock()
            return
        }
        wasHead := task.index == 0
        heap.Remove(p.pq, task.index)
        p.gaugeWaitQueueLength.Dec()
        if wasHead {
            if p.pq.Len() > 0 {
                p.timer.Reset(time.Until((*p.pq)[0].deadline))
            } else {
                p.timer.Stop()
            }
        }
        p.lock.Unlock()
        go func() {
            defer p.wg.Done()
            runTask(ctx, p.environment, "delay", task.fn)
            task.fn = nil
        }()
        p.taskCancelledCounter.Inc(ctx)
    })
    p.gaugeWaitQueueLength.Inc()
    p.lock.Unlock()
    return nil
}

func (p *DelayPoolImpl) Start(ctx context.Context) error {
    var called bool
    p.startOnce.Do(func() {
        p.lock.Lock()
        isDone := p.done
        p.lock.Unlock()
        if isDone {
            return
        }
        p.ctx = ctx
        called = true
    })
    if !called {
        p.lock.Lock()
        isDone := p.done
        p.lock.Unlock()
        if isDone {
            return ErrPoolStopped
        }
        return ErrPoolAlreadyStarted
    }
    return nil
}

func (p *DelayPoolImpl) Stop(ctx context.Context) {
    p.stopOnce.Do(func() {
        p.lock.Lock()
        p.done = true
        p.lock.Unlock()

        done := make(chan struct{})
        go func() {
            p.wg.Wait()
            close(done)
        }()
        select {
        case <-done:
            return
        case <-ctx.Done():
        }

        p.lock.Lock()
        if p.timer != nil {
            p.timer.Stop()
        }
        for p.pq.Len() > 0 {
            task := heap.Pop(p.pq).(*DelayTask)
            task.stopFn()
            p.gaugeWaitQueueLength.Dec()
            go func() {
                defer p.wg.Done()
                runTask(p.ctx, p.environment, "delay", task.fn)
                task.fn = nil
            }()
        }
        p.lock.Unlock()
        p.environment.Log().Warn(ctx, "delay pool stopped by timeout", log.Err(ctx.Err()))
        p.stopTimeoutCounter.Inc(ctx)
        p.wg.Wait()
    })
}
