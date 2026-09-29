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

func TestCppConnectorImplementationsSerializeIndependently(t *testing.T) {
	userver := DataConnectorImplementationUserverHTTP
	coro := DataConnectorImplementationGoogleGRPC
	encoded, err := json.Marshal(DataConnector{
		CppUserverImplementation: &userver,
		CppCoroImplementation:    &coro,
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
	if _, ok := fields["cppBoostImplementation"]; ok {
		t.Fatal("retired cppBoostImplementation must not be serialized")
	}
	if _, ok := fields["cppImplementation"]; ok {
		t.Fatal("legacy cppImplementation must not be serialized")
	}
	if got := string(fields["cppCoroImplementation"]); got != `"google/grpc"` {
		t.Fatalf("cppCoroImplementation = %s, want google/grpc", got)
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
