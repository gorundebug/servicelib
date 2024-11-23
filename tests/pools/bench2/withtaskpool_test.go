/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package bench2

import (
	"github.com/gorundebug/servicelib/tests/mockservice"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMain(m *testing.M) {
	mockservice.Main("../..", func() int {
		runtime.GOMAXPROCS(8)
		return m.Run()
	})
}

func BenchmarkWithTaskPool(b *testing.B) {
	service := mockservice.GetMockService()
	taskPool := service.GetTaskPool("Default")

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
				for j := 0; j < 100000; j++ {
					wg.Add(1)
					taskPool.AddTask(task)
				}
			}()
		}
		wg.Wait()
	}
}
