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

func newBenchGoroutineDelayPool(b *testing.B) DelayPool {
    b.Helper()
    m := testmetrics.New()
    rc, err := config.NewRuntimeConfig(&minimalConfig{})
    if err != nil {
        b.Fatal(err)
    }
    pool, err := makeGoroutineDelayPool(&mockPoolEnv{m: m, rc: rc})
    if err != nil {
        b.Fatal(err)
    }
    if err := pool.Start(context.Background()); err != nil {
        b.Fatal(err)
    }
    return pool
}

// BenchmarkDelayPool_Throughput measures throughput at several delay values.
// delay=0 is pure dispatch cost; delay>0 shows concurrent-wait behaviour where
// DelayPool (heap + single timer) may catch up to GoroutineDelayPool (N sleeping goroutines).
func BenchmarkDelayPool_Throughput(b *testing.B) {
    benchThroughput(b, newBenchDelayPool(b))
}

func BenchmarkGoroutineDelayPool_Throughput(b *testing.B) {
    benchThroughput(b, newBenchGoroutineDelayPool(b))
}

func benchThroughput(b *testing.B, pool DelayPool) {
    b.Helper()
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

// BenchmarkDelayPool_Burst_* measures how long it takes to add N tasks with a
// fixed delay and wait for all of them to complete. At high N the difference in
// goroutine and timer count becomes visible — this is where DelayPool should win.
func BenchmarkDelayPool_Burst(b *testing.B) {
    benchBurst(b, newBenchDelayPool(b))
}

func BenchmarkGoroutineDelayPool_Burst(b *testing.B) {
    benchBurst(b, newBenchGoroutineDelayPool(b))
}

func benchBurst(b *testing.B, pool DelayPool) {
    b.Helper()
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

// BenchmarkDelayPool_Cancel_* measures the context-cancellation path:
// tasks with a long deadline that are cancelled immediately.
// Both pools must run fn even on cancel (resource release semantics).
func BenchmarkDelayPool_Cancel(b *testing.B) {
    benchCancel(b, newBenchDelayPool(b))
}

func BenchmarkGoroutineDelayPool_Cancel(b *testing.B) {
    benchCancel(b, newBenchGoroutineDelayPool(b))
}

func benchCancel(b *testing.B, pool DelayPool) {
    b.Helper()
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

// BenchmarkDelayPool_SubmissionCost measures the cost of Delay() itself for
// different heap sizes. Uses a long deadline so tasks stay queued — this isolates
// heap management vs. goroutine-creation overhead without actual timer waits.
// The pool is stopped at the end of each sub-benchmark to drain all tasks.
func BenchmarkDelayPool_SubmissionCost(b *testing.B) {
    benchSubmission(b, func(b *testing.B) DelayPool { return newBenchDelayPool(b) })
}

func BenchmarkGoroutineDelayPool_SubmissionCost(b *testing.B) {
    benchSubmission(b, func(b *testing.B) DelayPool { return newBenchGoroutineDelayPool(b) })
}

func benchSubmission(b *testing.B, newPool func(*testing.B) DelayPool) {
    b.Helper()
    // Pre-populate the pool with this many waiting tasks before measuring.
    for _, preload := range []int{0, 100, 1_000, 10_000} {
        preload := preload
        b.Run(fmt.Sprintf("preload=%d", preload), func(b *testing.B) {
            // Shared cancellable context: cancel() wakes all waiting goroutines in
            // GoroutineDelayPool (via ctx.Done()) so Stop() returns immediately.
            ctx, cancel := context.WithCancel(context.Background())
            pool := newPool(b)
            const longDeadline = time.Hour

            for i := 0; i < preload; i++ {
                if err := pool.Delay(ctx, longDeadline, func() {}); err != nil {
                    b.Fatal(err)
                }
            }

            b.ReportAllocs()
            b.ResetTimer()

            for i := 0; i < b.N; i++ {
                if err := pool.Delay(ctx, longDeadline, func() {}); err != nil {
                    b.Fatal(err)
                }
            }

            b.StopTimer()
            cancel()
            pool.Stop(context.Background())
        })
    }
}
