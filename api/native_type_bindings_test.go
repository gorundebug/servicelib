package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeTypeBindingsStrictWithOrdinaryJSONUnmarshal(t *testing.T) {
	for _, body := range []string{
		`{"name":"Record","type":"custom","typeDefinitionLang1":"old"}`,
		`{"name":"Record","type":"custom","typeDefinitionLang2":null}`,
		`{"name":"Record","type":"custom","typeDefinitionLang7":"old"}`,
		`{"name":"Record","type":"custom","typeImportLang1":"old"}`,
		`{"name":"Record","type":"custom","typeImportLang2":null}`,
		`{"name":"Record","type":"custom","typeDefinition":"old"}`,
		`{"name":"Record","type":"custom","typeImport":null}`,
		`{"name":"Record","type":"custom","bindings":{"external":null}}`,
		`{"name":"Record","type":"custom","bindings":{"external":[]}}`,
		`{"name":"Record","type":"custom","bindings":{"external":{"definiton":null}}}`,
		`{"name":"Record","type":"custom","bindings":{"external":{"definition":5}}}`,
		`null`,
	} {
		t.Run(body, func(t *testing.T) {
			var app StreamApp
			if err := json.Unmarshal([]byte(`{"types":[`+body+`]}`), &app); err == nil {
				t.Fatal("ordinary request decoding silently accepted an invalid type")
			}
		})
	}
}

func TestNativeTypeBindingsJSONPresenceAndOpenTargets(t *testing.T) {
	const source = `{"types":[{"name":"Record","type":"custom","bindings":{"external.runtime":{"definition":"","import":"records.hpp","package":"records"},"cppCoro":{},"cppUserver":{"definition":"other::Record"},"go":{"definition":null}}}]}`
	var app StreamApp
	if err := json.Unmarshal([]byte(source), &app); err != nil {
		t.Fatal(err)
	}
	bindings := *app.Types[0].Bindings
	if len(bindings) != 4 || bindings["external.runtime"].Definition == nil || *bindings["external.runtime"].Definition != "" || bindings["cppCoro"].Definition != nil || bindings["go"].Definition != nil {
		t.Fatalf("authored binding presence changed: %#v", bindings)
	}
	encoded, err := json.Marshal(app)
	if err != nil {
		t.Fatal(err)
	}
	var restored StreamApp
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"definition":""`) || !strings.Contains(string(encoded), `"cppCoro":{}`) {
		t.Fatalf("empty object/definition lost: %s", encoded)
	}
}

func TestNativeTypeBindingJSONDoesNotInventBindings(t *testing.T) {
	for _, input := range []string{`{"name":"Record","type":"custom"}`, `{"name":"Record","type":"custom","bindings":null}`} {
		var value Type
		if err := json.Unmarshal([]byte(input), &value); err != nil {
			t.Fatal(err)
		}
		if value.Bindings != nil {
			t.Fatal("missing bindings became an authored dictionary")
		}
	}
	var value Type
	if err := json.Unmarshal([]byte(`{"name":"Record","type":"custom","bindings":{}}`), &value); err != nil {
		t.Fatal(err)
	}
	if value.Bindings == nil || len(*value.Bindings) != 0 {
		t.Fatal("explicit empty dictionary lost")
	}
}
