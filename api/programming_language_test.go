package api

import (
	"encoding/json"
	"testing"
)

func TestCppProgrammingLanguagesHaveStableDistinctValues(t *testing.T) {
	if ProgrammingLanguageCppUserver != ProgrammingLanguage(2) {
		t.Fatalf("CppUserver = %d, want stable value 2", ProgrammingLanguageCppUserver)
	}
	if ProgrammingLanguageCppBoost != ProgrammingLanguage(5) {
		t.Fatalf("CppBoost = %d, want value 5", ProgrammingLanguageCppBoost)
	}
}

func TestTypeScriptProgrammingLanguageHasStableValue(t *testing.T) {
	if ProgrammingLanguageTypeScript != ProgrammingLanguage(6) {
		t.Fatalf("TypeScript = %d, want value 6", ProgrammingLanguageTypeScript)
	}
}

func TestCppConnectorImplementationsSerializeIndependently(t *testing.T) {
	userver := DataConnectorImplementationUserverHTTP
	boost := DataConnectorImplementationBoostBeastHTTP
	encoded, err := json.Marshal(DataConnector{
		CppUserverImplementation: &userver,
		CppBoostImplementation:   &boost,
	})
	if err != nil {
		t.Fatal(err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["cppUserverImplementation"]; !ok {
		t.Fatal("cppUserverImplementation is absent")
	}
	if _, ok := fields["cppBoostImplementation"]; !ok {
		t.Fatal("cppBoostImplementation is absent")
	}
	if _, ok := fields["cppImplementation"]; ok {
		t.Fatal("legacy cppImplementation must not be serialized")
	}
}

func TestTypeScriptConnectorImplementationSerializesIndependently(t *testing.T) {
	implementation := DataConnectorImplementationNodeHTTP
	encoded, err := json.Marshal(DataConnector{
		TypeScriptImplementation: &implementation,
	})
	if err != nil {
		t.Fatal(err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["typeScriptImplementation"]; !ok {
		t.Fatal("typeScriptImplementation is absent")
	}
}
