/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package pool

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
)

func newBenchDelayPool(b *testing.B) DelayPool {
	b.Helper()
	m := testmetrics.New()
	rc, err := config.NewRuntimeConfig(&minimalConfig{})
	if err != nil {
		b.Fatal(err)
	}
	pool, err := makeDelayPool(&mockPoolEnv{m: m, rc: rc})
	if err != nil {
		b.Fatal(err)
	}
	if err := pool.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	return pool
}

// BenchmarkDelayPool_Throughput measures throughput at several delay values.
// delay=0 is pure dispatch cost; delay>0 shows behaviour under concurrent waits.
func BenchmarkDelayPool_Throughput(b *testing.B) {
	pool := newBenchDelayPool(b)
	defer pool.Stop(context.Background())
	ctx := context.Background()

	for _, delay := range []time.Duration{0, 500 * time.Microsecond, 5 * time.Millisecond} {
		delay := delay
		b.Run(fmt.Sprintf("delay=%v", delay), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			var wg sync.WaitGroup
			wg.Add(b.N)
			for i := 0; i < b.N; i++ {
				if err := pool.Delay(ctx, delay, func() { wg.Done() }); err != nil {
					b.Fatal(err)
				}
			}
			wg.Wait()
		})
	}
}

// BenchmarkDelayPool_Burst measures how long it takes to submit N tasks with a
// fixed delay and wait for all of them to complete.
func BenchmarkDelayPool_Burst(b *testing.B) {
	pool := newBenchDelayPool(b)
	defer pool.Stop(context.Background())
	ctx := context.Background()
	const delay = time.Millisecond

	for _, n := range []int{100, 1_000, 10_000} {
		n := n
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				wg.Add(n)
				for j := 0; j < n; j++ {
					if err := pool.Delay(ctx, delay, func() { wg.Done() }); err != nil {
						b.Fatal(err)
					}
				}
				wg.Wait()
			}
		})
	}
}

// BenchmarkDelayPool_Cancel measures the context-cancellation path:
// tasks with a long deadline that are cancelled immediately.
// fn must run even on cancel (resource release semantics).
func BenchmarkDelayPool_Cancel(b *testing.B) {
	pool := newBenchDelayPool(b)
	defer pool.Stop(context.Background())
	const longDelay = time.Hour

	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(b.N)
	cancels := make([]context.CancelFunc, b.N)
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancels[i] = cancel
		if err := pool.Delay(ctx, longDelay, func() { wg.Done() }); err != nil {
			b.Fatal(err)
		}
	}
	for _, cancel := range cancels {
		cancel()
	}
	wg.Wait()
}
