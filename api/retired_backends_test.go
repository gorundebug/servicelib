package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRetiredCppBoostJSONIsRejectedWithoutMutation(t *testing.T) {
	language := ProgrammingLanguageCppCoro
	err := json.Unmarshal([]byte(`5`), &language)
	if err == nil || !strings.Contains(err.Error(), RetiredCppBoostMessage) || language != ProgrammingLanguageCppCoro {
		t.Fatalf("retired language changed destination or lacked diagnostic: %v, %v", language, err)
	}
	for _, input := range []string{`{"cppBoostImplementation":"asio/grpc"}`, `{"cppBoostImplementation":null}`} {
		connector := DataConnector{Name: "preserved"}
		err := json.Unmarshal([]byte(input), &connector)
		if err == nil || !strings.Contains(err.Error(), RetiredCppBoostMessage) || connector.Name != "preserved" {
			t.Fatalf("retired connector changed destination or lacked diagnostic: %+v, %v", connector, err)
		}
	}
}

func TestCurrentCppBackendRoundTripPreservesStableIDsAndSelectors(t *testing.T) {
	for _, language := range []ProgrammingLanguage{ProgrammingLanguageCppUserver, ProgrammingLanguageTypeScript, ProgrammingLanguageCppCoro} {
		data, err := json.Marshal(language)
		if err != nil {
			t.Fatal(err)
		}
		var restored ProgrammingLanguage
		if err = json.Unmarshal(data, &restored); err != nil || restored != language {
			t.Fatalf("language round trip %d: %d, %v", language, restored, err)
		}
	}
	var connector DataConnector
	if err := json.Unmarshal([]byte(`{"name":"http","implementations":{"cppUserver":"userver/http","cppCoro":"boost/beast-http"}}`), &connector); err != nil {
		t.Fatal(err)
	}
	if connector.Implementations == nil || (*connector.Implementations)["cppCoro"] != "boost/beast-http" || (*connector.Implementations)["cppUserver"] != "userver/http" {
		t.Fatalf("current connector selectors lost: %+v", connector)
	}
}
