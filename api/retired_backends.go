package api

import (
	"encoding/json"
	"fmt"
)

const RetiredCppBoostMessage = "The cppBoost runtime has been removed. Migrate explicitly to CppCoro and adapt business functions to its coroutine API; automatic conversion would change the business-function contract."

// Keep the retired ID reserved; do not interpret a saved synchronous graph as
// a coroutine graph. Other invalid enum values retain normal schema validation.
func (language *ProgrammingLanguage) UnmarshalJSON(data []byte) error {
	type numericLanguage ProgrammingLanguage
	value := numericLanguage(*language)
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	if value == 5 {
		return fmt.Errorf("programmingLanguage: %s", RetiredCppBoostMessage)
	}
	*language = ProgrammingLanguage(value)
	return nil
}

// encoding/json otherwise discards removed fields without a diagnostic.
// Reject even a null old selector before replacing the destination object.
func (connector *DataConnector) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if _, present := fields["cppBoostImplementation"]; present {
		return fmt.Errorf("cppBoostImplementation: %s", RetiredCppBoostMessage)
	}
	type currentConnector DataConnector
	value := currentConnector(*connector)
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*connector = DataConnector(value)
	return nil
}
