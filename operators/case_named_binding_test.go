package operators

import (
	"context"
	"strings"
	"testing"

	"github.com/gorundebug/servicelib/runtime"
)

type namedCasePayload struct {
	terminal bool
}

type namedCaseCaller struct {
	consume func(context.Context, *namedCasePayload)
}

func (c namedCaseCaller) Consume(ctx context.Context, value *namedCasePayload) {
	c.consume(ctx, value)
}

func (namedCaseCaller) IsAsync() bool { return false }

// Exercise the actual Case.Build -> Case.Consume -> When.ConsumeCase path.
// Only the terminal caller is substituted; there is no transport or scheduler.
func TestNamedCaseBindingDispatchesSameTypedBranches(t *testing.T) {
	orders := [][]string{
		{"accepted", "terminal"},
		{"terminal", "accepted"},
		{"unrelated", "terminal", "accepted"},
	}
	for _, names := range orders {
		t.Run(strings.Join(names, "/"), func(t *testing.T) {
			builds := 0
			selector := BuildSwitchHandler[*namedCasePayload](func(_ runtime.Stream, branches []When) (func(*namedCasePayload) int, error) {
				builds++
				accepted, err := WhenIndex(branches, "accepted")
				if err != nil {
					return nil, err
				}
				terminal, err := WhenIndex(branches, "terminal")
				if err != nil {
					return nil, err
				}
				return func(value *namedCasePayload) int {
					if value.terminal {
						return terminal
					}
					return accepted
				}, nil
			})
			stream := &CaseStream[*namedCasePayload]{buildSwitch: selector}
			var selected string
			var received *namedCasePayload
			var receivedContext context.Context
			calls := 0
			for i, name := range names {
				branch := &WhenStream[*namedCasePayload, *namedCasePayload]{
					id: i + 1, name: name, caseStream: stream,
					caller: namedCaseCaller{consume: func(ctx context.Context, value *namedCasePayload) {
						selected, received, receivedContext = name, value, ctx
						calls++
					}},
				}
				if err := stream.AddStream(branch); err != nil {
					t.Fatal(err)
				}
			}
			if err := stream.Build(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			for _, terminal := range []bool{false, true} {
				value := &namedCasePayload{terminal: terminal}
				before := calls
				stream.Consume(ctx, value)
				want := "accepted"
				if terminal {
					want = "terminal"
				}
				if selected != want || received != value || receivedContext != ctx || calls != before+1 {
					t.Fatalf("wrong delivery: selected=%q want=%q payload_preserved=%t context_preserved=%t calls=%d",
						selected, want, received == value, receivedContext == ctx, calls-before)
				}
			}
			value := &namedCasePayload{terminal: true}
			if allocations := testing.AllocsPerRun(1000, func() { stream.Consume(ctx, value) }); allocations != 0 {
				t.Fatalf("dispatch allocations: %v", allocations)
			}
			if builds != 1 {
				t.Fatalf("selector rebuilt during dispatch: %d builds", builds)
			}
		})
	}

}

func TestNamedCaseBindingFailsAtBuild(t *testing.T) {
	for _, names := range [][]string{{"accepted"}, {"terminal", "terminal"}} {
		t.Run(strings.Join(names, "/"), func(t *testing.T) {
			stream := &CaseStream[*namedCasePayload]{
				buildSwitch: BuildSwitchHandler[*namedCasePayload](func(_ runtime.Stream, branches []When) (func(*namedCasePayload) int, error) {
					index, err := WhenIndex(branches, "terminal")
					if err != nil {
						return nil, err
					}
					return func(*namedCasePayload) int { return index }, nil
				}),
			}
			for i, name := range names {
				if err := stream.AddStream(&WhenStream[*namedCasePayload, *namedCasePayload]{id: i + 1, name: name, caseStream: stream}); err != nil {
					t.Fatal(err)
				}
			}
			if err := stream.Build(); err == nil {
				t.Fatal("invalid named branch binding accepted")
			}
		})
	}
}
