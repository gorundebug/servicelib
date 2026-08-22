package api

import _ "embed"

//go:embed serviceapi.yaml
var specData []byte

// Spec returns the raw OpenAPI spec (serviceapi.yaml) bytes.
// Used by consumers (e.g. servicegen) to derive enum metadata at init time.
func Spec() []byte {
    return specData
}
