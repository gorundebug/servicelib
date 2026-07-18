/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package bench3

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gorundebug/servicelib/tests/mockservice"
	"github.com/gorundebug/servicelib/tests/mockservice/config"
)

var testEnv *mockservice.TestEnv

func TestMain(m *testing.M) {
	mockservice.Main("../..", func(env *mockservice.TestEnv) int {
		testEnv = env
		runtime.GOMAXPROCS(16)
		return m.Run()
	})
}

func BenchmarkWithPriorityTaskPool(b *testing.B) {
	service := testEnv.Service
	taskPool := service.GetPriorityTaskPool(config.DefaultPriorityPoolName)

	var counter atomic.Int32

	wg := sync.WaitGroup{}

	task := func() {
		defer wg.Done()
		runtime.Gosched()
		counter.Add(1)
	}

	// A real, cancellable context (not context.Background()) so AddTask's
	// context.AfterFunc registration does actual work, matching production
	// traffic where callers pass a per-request/per-message context.
	// Created once, outside the benchmarked loop, so what's measured is
	// AddTask's per-task registration cost, not context construction cost.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 1000000; j++ {
					wg.Add(1)
					_ = taskPool.AddTask(ctx, 0, task)
				}
			}()
		}
		wg.Wait()
	}
}
