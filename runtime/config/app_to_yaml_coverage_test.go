/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package config

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/gorundebug/servicelib/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// appToYamlOnlyKeys are string literals used as map keys in app_to_yaml.go
// that have no corresponding field in api.StreamApp or its nested types.
var appToYamlOnlyKeys = map[string]string{
	"source":      "idSource serialised as stream name ref",
	"sources":     "idSources serialised as stream name refs",
	"endpoint":    "idEndpoint serialised as endpoint name ref",
	"pipelines":   "structural grouping section — not an api.StreamApp field",
	"errorStream": "frontend-only visual reference; not an api.Stream field",
}

// appToYamlExceptions lists api.StreamApp struct field JSON tag names that
// AppToYaml intentionally omits or serialises under a different key name.
var appToYamlExceptions = map[string]string{
	// IDs are auto-assigned on import; never written back.
	"id":              "auto-assigned, not written",
	"idService":       "derived from nesting under service section",
	"idDataConnector": "derived from nesting under dataConnector section",
	// ID-references written under renamed keys.
	"idSource":   `written as "source"`,
	"idSources":  `written as "sources"`,
	"idEndpoint": `written as "endpoint"`,
	// Streams are written into pipeline map values, not a top-level "streams" key.
	"streams": `written as pipeline map values, no "streams" key`,
	// pipeline is used as the pipelines map key, not a value field.
	"pipeline": "used as pipelines section map key",
	// Schema sentinel — never serialised.
	"undefinedEnum": "schema sentinel, not written",
}

// TestAppToYamlCoversStructFields verifies that app_to_yaml.go writes every
// field of api.StreamApp and its nested types as a YAML map key. It mirrors
// TestFromYamlCoversStructFields in servicegen but checks the serialisation
// direction.
func TestAppToYamlCoversStructFields(t *testing.T) {
	written := extractAppToYamlMapKeys(t)
	required := collectStructJSONTags(api.StreamApp{})

	var missing []string
	for field := range required {
		if appToYamlExceptions[field] != "" {
			continue
		}
		if written[field] == 0 {
			missing = append(missing, field)
		}
	}
	assert.Empty(t, missing, "AppToYaml does not write struct fields: %v", missing)

	// Reverse: keys written in app_to_yaml.go with no matching struct field
	// are either structural extras (listed in appToYamlOnlyKeys) or dead code.
	var orphaned []string
	for key := range written {
		if appToYamlOnlyKeys[key] != "" || appToYamlExceptions[key] != "" {
			continue
		}
		if required[key] == 0 {
			orphaned = append(orphaned, key)
		}
	}
	assert.Empty(t, orphaned, "app_to_yaml.go writes keys absent from structs (removed field?): %v", orphaned)
}

// extractAppToYamlMapKeys parses app_to_yaml.go and collects all string
// literals that appear as map index expressions or composite-literal map keys.
func extractAppToYamlMapKeys(t *testing.T) map[string]int {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "app_to_yaml.go", nil, 0)
	require.NoError(t, err)

	counts := map[string]int{}
	addLit := func(expr ast.Expr) {
		lit, ok := expr.(*ast.BasicLit)
		if ok && lit.Kind == token.STRING {
			if key, err := strconv.Unquote(lit.Value); err == nil {
				counts[key]++
			}
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.IndexExpr:
			addLit(v.Index)
		case *ast.KeyValueExpr:
			addLit(v.Key)
		}
		return true
	})
	return counts
}

// collectStructJSONTags walks all exported fields of v and its nested struct
// types recursively, counting how many distinct structs declare each JSON tag
// name.
func collectStructJSONTags(v interface{}) map[string]int {
	seen := map[reflect.Type]bool{}
	counts := map[string]int{}
	walkJSONFields(reflect.TypeOf(v), seen, counts)
	return counts
}

func walkJSONFields(t reflect.Type, seen map[reflect.Type]bool, counts map[string]int) {
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct || seen[t] {
		return
	}
	seen[t] = true
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("json")
		name := strings.Split(tag, ",")[0]
		if name != "" && name != "-" {
			counts[name]++
		}
		walkJSONFields(f.Type, seen, counts)
	}
}
