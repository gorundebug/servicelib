package operators

import "fmt"

// WhenIndex resolves a configured When name during BuildSwitch. Capture the
// returned index in the selector; do not repeat name lookup for each message.
// Reordering branches is safe, while removing or duplicating a required name
// fails construction. Renaming a branch requires updating the business binding.
func WhenIndex(items []When, name string) (int, error) {
	if name == "" {
		return 0, fmt.Errorf("case branch name must not be empty")
	}
	index := -1
	for i, item := range items {
		if item == nil {
			return 0, fmt.Errorf("case branch %d is nil", i)
		}
		if item.GetName() != name {
			continue
		}
		if index >= 0 {
			return 0, fmt.Errorf("case branch %q is ambiguous (indices %d and %d)", name, index, i)
		}
		index = i
	}
	if index < 0 {
		return 0, fmt.Errorf("case branch %q was not found", name)
	}
	return index, nil
}
