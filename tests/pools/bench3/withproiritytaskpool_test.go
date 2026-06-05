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

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 1000000; j++ {
					wg.Add(1)
					_ = taskPool.AddTask(context.Background(), 0, task)
				}
			}()
		}
		wg.Wait()
	}
}
