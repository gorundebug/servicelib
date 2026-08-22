package api

import (
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestLinkAsyncHasNoAPIDefault(t *testing.T) {
	var spec map[string]any
	if err := yaml.Unmarshal(Spec(), &spec); err != nil {
		t.Fatalf("parse embedded API schema: %v", err)
	}

	components := spec["components"].(map[string]any)
	schemas := components["schemas"].(map[string]any)
	link := schemas["Link"].(map[string]any)
	properties := link["properties"].(map[string]any)
	async := properties["async"].(map[string]any)
	if value, exists := async["default"]; exists {
		t.Fatalf("Link.async must be optional and have no API default, got %v", value)
	}
}
