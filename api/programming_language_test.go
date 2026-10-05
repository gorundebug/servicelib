package api

import (
	"encoding/json"
	"testing"
)

func TestCppProgrammingLanguagesHaveStableDistinctValues(t *testing.T) {
	if ProgrammingLanguageCppUserver != ProgrammingLanguage(2) {
		t.Fatalf("CppUserver = %d, want stable value 2", ProgrammingLanguageCppUserver)
	}
	if ProgrammingLanguageCppCoro != ProgrammingLanguage(7) {
		t.Fatalf("CppCoro = %d, want value 7", ProgrammingLanguageCppCoro)
	}
}

func TestTypeScriptProgrammingLanguageHasStableValue(t *testing.T) {
	if ProgrammingLanguageTypeScript != ProgrammingLanguage(6) {
		t.Fatalf("TypeScript = %d, want value 6", ProgrammingLanguageTypeScript)
	}
}

func TestConnectorImplementationsSerializeAsOneOpenMapping(t *testing.T) {
	bindings := map[string]string{"cppUserver": "userver/http", "cppCoro": "google/grpc", "typescript": "node/http", "external": "vendor/http"}
	encoded, err := json.Marshal(DataConnector{Implementations: &bindings})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	var restored map[string]string
	if err := json.Unmarshal(fields["implementations"], &restored); err != nil {
		t.Fatal(err)
	}
	if len(restored) != len(bindings) {
		t.Fatalf("lost connector bindings: %s", encoded)
	}
	for target, want := range bindings {
		if restored[target] != want {
			t.Fatalf("binding %s = %q, want %q", target, restored[target], want)
		}
	}
	for _, field := range []string{"goImplementation", "cppUserverImplementation", "cppCoroImplementation", "pythonImplementation", "rustImplementation", "typeScriptImplementation", "cppBoostImplementation", "cppImplementation"} {
		if _, exists := fields[field]; exists {
			t.Fatalf("retired selector %s serialized", field)
		}
	}
}

func TestRemovedConnectorSelectorsAreRejectedWithoutMutation(t *testing.T) {
	for _, field := range []string{"goImplementation", "cppUserverImplementation", "cppCoroImplementation", "pythonImplementation", "rustImplementation", "typeScriptImplementation"} {
		for _, value := range []string{`null`, `"unused"`} {
			connector := DataConnector{Name: "preserved"}
			if err := json.Unmarshal([]byte(`{"`+field+`":`+value+`}`), &connector); err == nil {
				t.Fatalf("removed selector accepted: %s", field)
			}
			if connector.Name != "preserved" {
				t.Fatalf("failed decode mutated connector: %+v", connector)
			}
		}
	}
	var connector DataConnector
	if err := json.Unmarshal([]byte(`{"name":"uses pack defaults","type":1}`), &connector); err != nil {
		t.Fatal(err)
	}
	if connector.Implementations != nil {
		t.Fatalf("API decode must not invent target selections: %+v", connector)
	}
}
