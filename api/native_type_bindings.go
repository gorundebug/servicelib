package api

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Decode the authored type contract strictly even when the enclosing request is
// read with json.Unmarshal. Unknown fields must not silently erase native types.
// This runs at the input boundary, not while preparing or rendering templates.
func (value *Type) UnmarshalJSON(data []byte) error {
	type typeDocument Type
	document := typeDocument(*value)
	if err := decodeTypeObject(data, &document, "type"); err != nil {
		return err
	}
	*value = Type(document)
	return nil
}

func (value *NativeTypeBinding) UnmarshalJSON(data []byte) error {
	type bindingDocument NativeTypeBinding
	document := bindingDocument(*value)
	if err := decodeTypeObject(data, &document, "native type binding"); err != nil {
		return err
	}
	*value = NativeTypeBinding(document)
	return nil
}

func decodeTypeObject(data []byte, value any, kind string) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("%s must be an object, not null", kind)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(value)
}
