package api

import (
	"encoding/json"
	"testing"
)

func TestRetiredBackendGuardPreservesPartialJSONDecode(t *testing.T) {
	language := ProgrammingLanguageCppCoro
	if err := json.Unmarshal([]byte("null"), &language); err != nil || language != ProgrammingLanguageCppCoro {
		t.Fatalf("null changed language: %v, %v", language, err)
	}
	implementation := DataConnectorImplementationGoogleGRPC
	connector := DataConnector{Id: 42, Name: "Original", CppCoroImplementation: &implementation}
	if err := json.Unmarshal([]byte(`{"name":"Updated"}`), &connector); err != nil {
		t.Fatal(err)
	}
	if connector.Id != 42 || connector.Name != "Updated" || connector.CppCoroImplementation == nil || *connector.CppCoroImplementation != implementation {
		t.Fatalf("partial decode changed omitted fields: %+v", connector)
	}
	if err := json.Unmarshal([]byte("null"), &connector); err != nil || connector.Id != 42 || connector.Name != "Updated" {
		t.Fatalf("null changed connector: %+v, %v", connector, err)
	}
}
