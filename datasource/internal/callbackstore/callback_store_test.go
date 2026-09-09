package callbackstore

import (
	"math/rand"
	"testing"
)

func TestCallbackStoreMatchesMap(t *testing.T) {
	var store Store[int]
	reference := make(map[string]int)
	keys := []string{"", "first", "second", "third"}
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 10000; i++ {
		key := keys[rng.Intn(len(keys))]
		switch rng.Intn(3) {
		case 0:
			value := rng.Intn(5)
			store.Set(key, value)
			reference[key] = value
		case 1:
			_, exists := reference[key]
			if store.Remove(key) != exists {
				t.Fatalf("delete %q diverged at %d", key, i)
			}
			delete(reference, key)
		case 2:
			got, ok := store.Get(key)
			want, exists := reference[key]
			if got != want || ok != exists {
				t.Fatalf("lookup %q diverged at %d", key, i)
			}
		}
	}
}

func TestCallbackStoreRetainsCallableAndNilRegistration(t *testing.T) {
	var store Store[func() int]
	calls := 0
	store.Set("", func() int { calls++; return calls })
	for want := 1; want <= 2; want++ {
		callback, ok := store.Get("")
		if !ok || callback() != want {
			t.Fatal("callable state lost")
		}
	}
	store.Set("", nil)
	callback, ok := store.Get("")
	if !ok || callback != nil {
		t.Fatal("nil registration must remain present")
	}
	store.Set("other", func() int { return 3 })
	callback, ok = store.Get("")
	if !ok || callback != nil {
		t.Fatal("promotion lost nil registration")
	}
	if !store.Remove("") || store.Remove("") {
		t.Fatal("duplicate removal changed")
	}
}

var StoreSink any

func BenchmarkSingleCallbackStorage(b *testing.B) {
	b.Run("map", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			state := &struct{ callbacks map[string]func() }{make(map[string]func())}
			state.callbacks["result"] = nil
			StoreSink = state
		}
	})
	b.Run("inline", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			state := new(Store[func()])
			state.Set("result", nil)
			StoreSink = state
		}
	})
}
