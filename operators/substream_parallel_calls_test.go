package operators

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/stretchr/testify/require"
)

type pendingParallelSubStreamResult struct {
	call int
	ctx  context.Context
	out  runtime.Collect[int]
}

type parallelSubStreamReport struct {
	values [2][]int
	errors [2]error
	caller [2]bool
}

// The actual outer Map business function forks two calls to the same entry,
// using the exact same context (including stream ID), then joins both calls.
func TestSubStreamBusinessFunctionForkJoinIsolation(t *testing.T) {
	for _, test := range []struct {
		name  string
		order []int
	}{
		{"first_then_second", []int{0, 0, 0, 1, 1, 1}},
		{"second_then_first", []int{1, 1, 1, 0, 0, 0}},
		{"interleaved", []int{0, 1, 1, 1, 0, 0}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ctx = runtime.WithStreamId(ctx, "one-shared-parent-request")
			pending := make(chan pendingParallelSubStreamResult, 2)
			inner := newTestSubStream(t, func(callCtx context.Context, _ runtime.Stream, value int, out runtime.Collect[int]) {
				pending <- pendingParallelSubStreamResult{call: value, ctx: callCtx, out: out}
			})
			reports := make(chan parallelSubStreamReport, 1)
			outer := newTestSubStream(t, func(parent context.Context, _ runtime.Stream, _ int, out runtime.Collect[int]) {
				var report parallelSubStreamReport
				var calls sync.WaitGroup
				for i := range report.values {
					calls.Add(1)
					go func() {
						defer calls.Done()
						report.caller[i] = true
						report.errors[i] = inner.Consume(parent, i, runtime.SubStreamCollectorFunc[int](func(returnCtx context.Context, value int) bool {
							report.caller[i] = report.caller[i] && returnCtx == parent
							report.values[i] = append(report.values[i], value)
							return len(report.values[i]) == 2
						}))
					}()
				}
				calls.Wait()
				reports <- report
				sum := 0
				for _, values := range report.values {
					for _, value := range values {
						sum += value
					}
				}
				out.Out(parent, sum)
			})
			done := make(chan struct{})
			var callErr error
			got, outerCalls := 0, 0
			go func() {
				defer close(done)
				callErr = outer.Consume(ctx, 0, runtime.SubStreamCollectorFunc[int](func(returnCtx context.Context, value int) bool {
					if returnCtx != ctx {
						t.Error("outer invocation context was not restored")
					}
					got = value
					outerCalls++
					return true
				}))
			}()
			var jobs [2]pendingParallelSubStreamResult
			for range jobs {
				select {
				case job := <-pending:
					jobs[job.call] = job
				case <-ctx.Done():
					t.Fatal("both calls did not enter the SubStream")
				}
			}
			for _, job := range jobs {
				sid, ok := runtime.StreamIdFromContext(job.ctx)
				require.True(t, ok)
				require.Equal(t, "one-shared-parent-request", sid.GetID())
			}
			var sequence [2]int
			for _, index := range test.order {
				sequence[index]++
				job := jobs[index]
				// The third delivery is late, sometimes while the sibling is
				// still waiting. It must never reach either collector.
				job.out.Out(job.ctx, (index+1)*100+sequence[index])
			}
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("business function did not finish both SubStream calls")
			}
			report := <-reports
			require.NoError(t, callErr)
			for i := range report.values {
				require.NoError(t, report.errors[i])
				require.True(t, report.caller[i])
			}
			require.Equal(t, [2][]int{{101, 102}, {201, 202}}, report.values)
			require.Equal(t, 606, got)
			require.Equal(t, 1, outerCalls)
		})
	}
}

func TestSubStreamSiblingCancellationIsIndependent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = runtime.WithStreamId(ctx, "shared-request")
	firstCtx, cancelFirst := context.WithCancel(ctx)
	defer cancelFirst()
	pending := make(chan pendingParallelSubStreamResult, 2)
	entry := newTestSubStream(t, func(callCtx context.Context, _ runtime.Stream, value int, out runtime.Collect[int]) {
		pending <- pendingParallelSubStreamResult{call: value, ctx: callCtx, out: out}
	})
	firstFinished := make(chan error, 1)
	done := make(chan parallelSubStreamReport, 1)
	go func() {
		var report parallelSubStreamReport
		var calls sync.WaitGroup
		for i, callCtx := range []context.Context{firstCtx, ctx} {
			calls.Add(1)
			go func() {
				defer calls.Done()
				report.errors[i] = entry.Consume(callCtx, i, runtime.SubStreamCollectorFunc[int](func(returnCtx context.Context, value int) bool {
					report.caller[i] = returnCtx == callCtx
					report.values[i] = append(report.values[i], value)
					return true
				}))
				if i == 0 {
					firstFinished <- report.errors[i]
				}
			}()
		}
		calls.Wait()
		done <- report
	}()
	var jobs [2]pendingParallelSubStreamResult
	for range jobs {
		select {
		case job := <-pending:
			jobs[job.call] = job
		case <-ctx.Done():
			t.Fatal("calls did not start")
		}
	}
	cancelFirst()
	select {
	case err := <-firstFinished:
		require.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal("canceled call did not return")
	}
	jobs[0].out.Out(jobs[0].ctx, 99)
	jobs[1].out.Out(jobs[1].ctx, 42)
	select {
	case report := <-done:
		require.Empty(t, report.values[0])
		require.ErrorIs(t, report.errors[0], context.Canceled)
		require.NoError(t, report.errors[1])
		require.Equal(t, []int{42}, report.values[1])
		require.True(t, report.caller[1])
	case <-ctx.Done():
		t.Fatal("sibling call did not complete")
	}
}

func TestSubStreamSiblingCollectorsCanOverlap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = runtime.WithStreamId(ctx, "shared-request")
	pending := make(chan pendingParallelSubStreamResult, 2)
	entered := make(chan int, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	entry := newTestSubStream(t, func(callCtx context.Context, _ runtime.Stream, value int, out runtime.Collect[int]) {
		pending <- pendingParallelSubStreamResult{call: value, ctx: callCtx, out: out}
	})
	done := make(chan parallelSubStreamReport, 1)
	go func() {
		var report parallelSubStreamReport
		var calls sync.WaitGroup
		for i := range report.values {
			calls.Add(1)
			go func() {
				defer calls.Done()
				report.errors[i] = entry.Consume(ctx, i, runtime.SubStreamCollectorFunc[int](func(returnCtx context.Context, value int) bool {
					entered <- i
					select {
					case <-release:
					case <-ctx.Done():
						return false
					}
					report.caller[i] = returnCtx == ctx
					report.values[i] = append(report.values[i], value)
					return true
				}))
			}()
		}
		calls.Wait()
		done <- report
	}()
	var deliveries sync.WaitGroup
	for range 2 {
		select {
		case job := <-pending:
			deliveries.Add(1)
			go func() { defer deliveries.Done(); job.out.Out(job.ctx, job.call+10) }()
		case <-ctx.Done():
			t.Fatal("calls did not start")
		}
	}
	var seen [2]bool
	for range seen {
		select {
		case i := <-entered:
			require.False(t, seen[i], "one collector received both results")
			seen[i] = true
		case <-ctx.Done():
			t.Fatal("different invocations serialized their collectors")
		}
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case report := <-done:
		for i := range report.values {
			require.NoError(t, report.errors[i])
			require.True(t, report.caller[i])
			require.Equal(t, []int{i + 10}, report.values[i])
		}
	case <-ctx.Done():
		t.Fatal("calls did not finish after releasing collectors")
	}
	deliveries.Wait()
}
