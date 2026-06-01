/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package config

import "github.com/gorundebug/servicelib/api"

// TypeConfig holds the metadata for a named data type referenced by stream nodes.
// TypeDefinition and TypeImport are the Go-specific fields from TypeDefinitionLang1/TypeImportLang1.
type TypeConfig struct {
	Name             string                   `yaml:"name" mapstructure:"name"`
	Type             api.DataType             `yaml:"type" mapstructure:"type"`
	TypeDefinition   string                   `yaml:"typeDefinition,omitempty" mapstructure:"typeDefinition"`
	TypeImport       string                   `yaml:"typeImport,omitempty" mapstructure:"typeImport"`
	ValueType        string                   `yaml:"valueType,omitempty" mapstructure:"valueType"`
	KeyType          string                   `yaml:"keyType,omitempty" mapstructure:"keyType"`
	Package          string                   `yaml:"package,omitempty" mapstructure:"package"`
	Module           string                   `yaml:"module,omitempty" mapstructure:"module"`
	DefinitionFormat api.TypeDefinitionFormat `yaml:"definitionFormat,omitempty" mapstructure:"definitionFormat"`
	PublicType       bool                     `yaml:"publicType,omitempty" mapstructure:"publicType"`
	TransferByValue  bool                     `yaml:"transferByValue,omitempty" mapstructure:"transferByValue"`
	UseAlias         bool                     `yaml:"useAlias,omitempty" mapstructure:"useAlias"`
	Properties       map[string]interface{}   `yaml:",inline" mapstructure:",remain"`
}

func (t *TypeConfig) GetProperty(name string) interface{} {
	return t.Properties[name]
}
