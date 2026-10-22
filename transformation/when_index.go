package transformation

import "github.com/gorundebug/servicelib/operators"

// WhenIndex binds a configured branch name once, inside BuildSwitch.
// The returned index is an execution detail, not a positional business contract.
func WhenIndex(items []When, name string) (int, error) {
	return operators.WhenIndex(items, name)
}
