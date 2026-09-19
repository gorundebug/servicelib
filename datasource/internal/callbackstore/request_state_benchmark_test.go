package callbackstore_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/datasource/internal/callbackstore"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/store"
)

// This fixture isolates endpoint request correlation, not transport throughput.
// Both paths retain callback storage, lifecycle locking, Done and callback removal.
// Stream IDs are prepared per worker and reused only after request completion;
// ID generation, network I/O, metrics and tracing are intentionally excluded.
type requestBenchmarkState struct {
	mu        sync.RWMutex
	cbMu      sync.Mutex
	once      sync.Once
	done      chan struct{}
	callbacks callbackstore.Store[func(*requestBenchmarkState) bool]
	closed    bool
	received  int
	expected  int
}

type requestBenchmarkKey struct{ marker byte }
type requestBenchmarkDepthKey int

type requestBenchmarkHarness struct {
	pending *store.RotatingMap[string, *requestBenchmarkState]
	key     *requestBenchmarkKey
}

func newRequestBenchmarkHarness(tb testing.TB, useContext bool) *requestBenchmarkHarness {
	h := &requestBenchmarkHarness{key: &requestBenchmarkKey{}}
	if !useContext {
		h.pending = store.MakeRotatingMap[string, *requestBenchmarkState](30 * time.Second)
		if err := h.pending.Start(context.Background()); err != nil {
			tb.Fatal(err)
		}
		tb.Cleanup(func() { h.pending.Stop(context.Background()) })
	}
	return h
}

func requestBenchmarkCallback(s *requestBenchmarkState) bool {
	s.received++
	if s.received == s.expected {
		s.once.Do(func() { close(s.done) })
	}
	return true
}

func (h *requestBenchmarkHarness) begin(parent context.Context, id string, ids []string, depth int) (context.Context, *requestBenchmarkState) {
	s := &requestBenchmarkState{done: make(chan struct{}), expected: len(ids)}
	for _, messageID := range ids {
		s.cbMu.Lock()
		s.callbacks.Set(messageID, requestBenchmarkCallback)
		s.cbMu.Unlock()
	}
	ctx := parent
	if h.pending != nil {
		if err := h.pending.Set(id, s); err != nil {
			panic(err)
		}
	} else {
		ctx = context.WithValue(ctx, h.key, s)
	}
	// Put unrelated values ABOVE the correlation entry, so lookups traverse
	// the requested depth in both variants. Include their allocation cost.
	for i := 0; i < depth; i++ {
		ctx = context.WithValue(ctx, requestBenchmarkDepthKey(i), i)
	}
	return ctx, s
}

func (h *requestBenchmarkHarness) deliver(ctx context.Context, messageID string) bool {
	var s *requestBenchmarkState
	var id string
	if h.pending != nil {
		sid, ok := runtime.StreamIdFromContext(ctx)
		if !ok {
			return false
		}
		id = sid.GetID()
		var found bool
		s, found = h.pending.Get(id)
		if !found {
			return false
		}
	} else {
		var found bool
		s, found = ctx.Value(h.key).(*requestBenchmarkState)
		if !found {
			return false
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if h.pending != nil {
		// Match the endpoint's second lookup under the lifecycle lock.
		if current, found := h.pending.Get(id); !found || current != s {
			return false
		}
	} else if s.closed {
		return false
	}
	s.cbMu.Lock()
	callback, found := s.callbacks.Get(messageID)
	s.cbMu.Unlock()
	if !found || callback == nil {
		return false
	}
	if callback(s) {
		s.cbMu.Lock()
		s.callbacks.Remove(messageID)
		s.cbMu.Unlock()
	}
	return true
}

func (h *requestBenchmarkHarness) finish(id string, s *requestBenchmarkState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if h.pending != nil {
		h.pending.Pop(id)
	} else {
		s.closed = true
	}
}

func TestEndpointRequestStateLifecycle(t *testing.T) {
	for _, useContext := range []bool{false, true} {
		t.Run(fmt.Sprintf("context=%t", useContext), func(t *testing.T) {
			h := newRequestBenchmarkHarness(t, useContext)
			ids := []string{"first", "second"}
			parent := runtime.WithStreamId(context.Background(), "request")
			ctx, s := h.begin(parent, "request", ids, 8)
			if h.deliver(context.Background(), "first") || h.deliver(ctx, "unknown") {
				t.Fatal("unexpected delivery for missing correlation or callback")
			}
			for _, id := range ids {
				if !h.deliver(ctx, id) || h.deliver(ctx, id) {
					t.Fatal("callback must be invoked exactly once")
				}
			}
			select {
			case <-s.done:
			default:
				t.Fatal("request did not complete")
			}
			h.finish("request", s)
			// Reusing the ID must not deliver a stale context into a new state.
			// The map variant only promises isolation while the lifecycle lock
			// protects the looked-up state; production IDs should remain unique.
			if h.deliver(ctx, "first") || s.received != len(ids) {
				t.Fatal("late result changed completed state")
			}
			// A closed request with a still-registered callback must reject it.
			ctx, s = h.begin(parent, "request", ids, 8)
			h.finish("request", s)
			if h.deliver(ctx, "first") || s.received != 0 {
				t.Fatal("closed request accepted a result")
			}
		})
	}
}

// One operation is an entire request, including all results and completion.
// Parallel mode shares the actual sharded RotatingMap across workers. Each
// worker delivers its own results sequentially, as a minimal endpoint callback.
func BenchmarkEndpointRequestState(b *testing.B) {
	for _, depth := range []int{0, 5, 8, 32} {
		for _, results := range []int{1, 16} {
			for _, parallel := range []bool{false, true} {
				for _, mode := range []string{"rotating_map", "context"} {
					name := fmt.Sprintf("depth=%d/results=%d/parallel=%t/%s", depth, results, parallel, mode)
					b.Run(name, func(b *testing.B) {
						h := newRequestBenchmarkHarness(b, mode == "context")
						ids := make([]string, results)
						for i := range ids {
							ids[i] = fmt.Sprintf("message-%d", i)
						}
						var workerID atomic.Uint64
						runWorker := func(next func() bool) {
							id := fmt.Sprintf("request-%020d", workerID.Add(1))
							parent := runtime.WithStreamId(context.Background(), id)
							for next() {
								ctx, s := h.begin(parent, id, ids, depth)
								for _, messageID := range ids {
									if !h.deliver(ctx, messageID) {
										panic("result was not delivered")
									}
								}
								<-s.done
								h.finish(id, s)
								if s.received != results {
									panic("incorrect result count")
								}
							}
						}
						b.ReportAllocs()
						b.ResetTimer()
						if parallel {
							b.RunParallel(func(pb *testing.PB) { runWorker(pb.Next) })
						} else {
							i := 0
							runWorker(func() bool { i++; return i <= b.N })
						}
					})
				}
			}
		}
	}
}

// Measure lookup alone with five unrelated context values above the request
// correlation entry. All state and context allocation happens before timing.
// map_known_id isolates Get; stream_id_and_map includes the endpoint's ID lookup.
// This excludes the subsequent lifecycle lock, second Get and callback dispatch.
func BenchmarkEndpointRequestLookupDepth5(b *testing.B) {
	for _, mode := range []string{"map_known_id", "stream_id_and_map", "context"} {
		b.Run(mode, func(b *testing.B) {
			h := newRequestBenchmarkHarness(b, mode == "context")
			const id = "request-00000000000000000001"
			parent := runtime.WithStreamId(context.Background(), id)
			ctx, want := h.begin(parent, id, []string{"result"}, 5)
			b.Cleanup(func() { h.finish(id, want) })
			b.ReportAllocs()
			switch mode {
			case "map_known_id":
				for b.Loop() {
					got, ok := h.pending.Get(id)
					if !ok || got != want {
						b.Fatal("incorrect request state")
					}
				}
			case "stream_id_and_map":
				for b.Loop() {
					sid, ok := runtime.StreamIdFromContext(ctx)
					if !ok {
						b.Fatal("missing stream ID")
					}
					got, ok := h.pending.Get(sid.GetID())
					if !ok || got != want {
						b.Fatal("incorrect request state")
					}
				}
			case "context":
				for b.Loop() {
					got, ok := ctx.Value(h.key).(*requestBenchmarkState)
					if !ok || got != want {
						b.Fatal("incorrect request state")
					}
				}
			}
		})
	}
}
