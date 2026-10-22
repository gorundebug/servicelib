package operators_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/transformation"
)

type namedBranch string

func (b namedBranch) GetName() string               { return string(b) }
func (namedBranch) GetType() reflect.Type           { return reflect.TypeOf(false) }
func (namedBranch) GetWhenConsumer() runtime.Stream { return nil }

func bindTerminal(items []transformation.When) (func(bool) int, error) {
	accepted, err := transformation.WhenIndex(items, "accepted")
	if err != nil {
		return nil, err
	}
	terminal, err := transformation.WhenIndex(items, "terminal")
	if err != nil {
		return nil, err
	}
	return func(value bool) int {
		if value {
			return terminal
		}
		return accepted
	}, nil
}

func TestWhenIndexPreservesOutcomesAfterReordering(t *testing.T) {
	for _, names := range [][]string{{"accepted", "terminal"}, {"terminal", "accepted"}, {"unrelated", "terminal", "accepted"}} {
		items := make([]transformation.When, len(names))
		for i, name := range names {
			items[i] = namedBranch(name)
		}
		selectBranch, err := bindTerminal(items)
		if err != nil {
			t.Fatal(err)
		}
		for _, terminal := range []bool{false, true} {
			want := "accepted"
			if terminal {
				want = "terminal"
			}
			if got := items[selectBranch(terminal)].GetName(); got != want {
				t.Fatalf("order=%v terminal=%v: got %q, want %q", names, terminal, got, want)
			}
		}
		if allocs := testing.AllocsPerRun(1000, func() { _ = selectBranch(true) }); allocs != 0 {
			t.Fatalf("selector allocated %v times", allocs)
		}
	}
}

func TestWhenIndexRejectsInvalidBinding(t *testing.T) {
	for _, tc := range []struct {
		name    string
		items   []transformation.When
		branch  string
		message string
	}{
		{"missing", []transformation.When{namedBranch("accepted")}, "terminal", "not found"},
		{"duplicate", []transformation.When{namedBranch("terminal"), namedBranch("terminal")}, "terminal", "ambiguous"},
		{"empty", nil, "", "must not be empty"},
		{"nil branch", []transformation.When{nil}, "terminal", "is nil"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := transformation.WhenIndex(tc.items, tc.branch)
			if err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("got %v; want error containing %q", err, tc.message)
			}
		})
	}
}
