package kafka

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestResultCallbackConcurrentRegistration(t *testing.T) {
	result := &kafkaResult[int, int, int, int]{}
	var workers sync.WaitGroup
	for i := 0; i < 32; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			id := fmt.Sprint(i)
			result.SetResultCallback(id, func(_ context.Context, _ StreamContext[int, int, int], state int, value int) bool {
				return state == i && value == i
			})
			result.cbMu.Lock()
			callback, ok := result.messageCallbackMap.Get(id)
			result.cbMu.Unlock()
			if !ok || callback == nil || !callback(context.Background(), StreamContext[int, int, int]{}, i, i) {
				t.Errorf("callback %s was lost or replaced", id)
			}
			result.cbMu.Lock()
			removed := result.messageCallbackMap.Remove(id)
			duplicate := result.messageCallbackMap.Remove(id)
			result.cbMu.Unlock()
			if !removed || duplicate {
				t.Errorf("incorrect removal for %s", id)
			}
		}(i)
	}
	workers.Wait()
	result.SetResultCallback("", nil)
	result.cbMu.Lock()
	callback, ok := result.messageCallbackMap.Get("")
	result.cbMu.Unlock()
	if !ok || callback != nil {
		t.Fatal("nil callback registration must remain present")
	}
}
