/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the LICENSE file for details.
 */

package pool

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
)

func newBenchTaskPool(b *testing.B, name string, executors int) TaskPool {
	b.Helper()

	m := testmetrics.New()
	poolConfig := &config.PoolConfig{Name: name, ExecutorsCount: executors}
	runtimeConfig, err := config.NewRuntimeConfig(
		&minimalConfig{pools: []*config.PoolConfig{poolConfig}},
	)
	if err != nil {
		b.Fatal(err)
	}

	taskPool, err := makeTaskPool(
		&mockPoolEnv{m: m, rc: runtimeConfig},
		poolConfig,
	)
	if err != nil {
		b.Fatal(err)
	}
	if err := taskPool.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	return taskPool
}

func benchmarkTaskPoolEndToEnd(b *testing.B, withDeadline bool) {
	for _, executors := range []int{1, 2, 4, 8} {
		for _, batchSize := range []int{1, 256, 1024} {
			name := fmt.Sprintf("executors=%d/batch=%d", executors, batchSize)
			b.Run(name, func(b *testing.B) {
				b.StopTimer()
				pool := newBenchTaskPool(b, "benchmark-task-pool", executors)
				b.Cleanup(func() { pool.Stop(context.Background()) })
				b.ReportAllocs()
				b.StartTimer()

				for range b.N {
					var remaining atomic.Int64
					remaining.Store(int64(batchSize))
					completed := make(chan struct{})

					ctx := context.Background()
					cancel := func() {}
					if withDeadline {
						ctx, cancel = context.WithDeadline(ctx, time.Now().Add(time.Hour))
					}

					for range batchSize {
						if err := pool.AddTask(ctx, func() {
							if remaining.Add(-1) == 0 {
								close(completed)
							}
						}); err != nil {
							cancel()
							b.Fatal(err)
						}
					}

					<-completed
					cancel()
				}

				b.StopTimer()
				processed := float64(b.N * batchSize)
				b.ReportMetric(processed/b.Elapsed().Seconds(), "tasks/s")
			})
		}
	}
}

// BenchmarkTaskPool_EnqueueAndExecute measures the end-to-end path from
// AddTask through execution and completion notification.
func BenchmarkTaskPool_EnqueueAndExecute(b *testing.B) {
	benchmarkTaskPoolEndToEnd(b, false)
}

// BenchmarkTaskPool_EnqueueAndExecuteWithDeadline measures the same path when
// every accepted task registers context deadline handling.
func BenchmarkTaskPool_EnqueueAndExecuteWithDeadline(b *testing.B) {
	benchmarkTaskPoolEndToEnd(b, true)
}
