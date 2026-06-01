/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package config

import "github.com/gorundebug/servicelib/api"

// StreamConfig is the interface for all stream types.
type StreamConfig interface {
	GetID() int
	GetName() string
	GetPipeline() string
	GetType() api.TransformationType
	GetIdService() int
	GetIdSource() int
	GetIdSources() []int
	GetXPos() float64
	GetYPos() float64
	GetProperty(name string) interface{}
}

// InputStreamConfig is the configuration for the Input transformation.
type InputStreamConfig struct {
	ID         int                    `yaml:"id" mapstructure:"id"`
	Name       string                 `yaml:"name" mapstructure:"name"`
	Pipeline   string                 `yaml:"pipeline" mapstructure:"pipeline"`
	IdService  int                    `yaml:"idService" mapstructure:"idService"`
	IdSource   int                    `yaml:"idSource" mapstructure:"idSource"`
	IdSources  []int                  `yaml:"idSources" mapstructure:"idSources"`
	XPos       float64                `yaml:"xPos" mapstructure:"xPos"`
	YPos       float64                `yaml:"yPos" mapstructure:"yPos"`
	ValueType  string                 `yaml:"valueType,omitempty" mapstructure:"valueType"`
	IdEndpoint int                    `yaml:"idEndpoint,omitempty" mapstructure:"idEndpoint"`
	Properties map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *InputStreamConfig) GetID() int                          { return s.ID }
func (s *InputStreamConfig) GetName() string                     { return s.Name }
func (s *InputStreamConfig) GetPipeline() string                 { return s.Pipeline }
func (s *InputStreamConfig) GetType() api.TransformationType     { return api.TransformationTypeInput }
func (s *InputStreamConfig) GetIdService() int                   { return s.IdService }
func (s *InputStreamConfig) GetIdSource() int                    { return s.IdSource }
func (s *InputStreamConfig) GetIdSources() []int                 { return s.IdSources }
func (s *InputStreamConfig) GetXPos() float64                    { return s.XPos }
func (s *InputStreamConfig) GetYPos() float64                    { return s.YPos }
func (s *InputStreamConfig) GetProperty(name string) interface{} { return s.Properties[name] }

// MapStreamConfig is the configuration for the Map transformation.
type MapStreamConfig struct {
	ID                       int                    `yaml:"id" mapstructure:"id"`
	Name                     string                 `yaml:"name" mapstructure:"name"`
	Pipeline                 string                 `yaml:"pipeline" mapstructure:"pipeline"`
	IdService                int                    `yaml:"idService" mapstructure:"idService"`
	IdSource                 int                    `yaml:"idSource" mapstructure:"idSource"`
	XPos                     float64                `yaml:"xPos" mapstructure:"xPos"`
	YPos                     float64                `yaml:"yPos" mapstructure:"yPos"`
	ValueType                string                 `yaml:"valueType,omitempty" mapstructure:"valueType"`
	FunctionPackage          string                 `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	FunctionName             string                 `yaml:"functionName,omitempty" mapstructure:"functionName"`
	PublicFunction           bool                   `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	FunctionDescription      string                 `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup string                 `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	FunctionModule           string                 `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	Properties               map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *MapStreamConfig) GetID() int                          { return s.ID }
func (s *MapStreamConfig) GetName() string                     { return s.Name }
func (s *MapStreamConfig) GetPipeline() string                 { return s.Pipeline }
func (s *MapStreamConfig) GetType() api.TransformationType     { return api.TransformationTypeMap }
func (s *MapStreamConfig) GetIdService() int                   { return s.IdService }
func (s *MapStreamConfig) GetIdSource() int                    { return s.IdSource }
func (s *MapStreamConfig) GetIdSources() []int                 { return nil }
func (s *MapStreamConfig) GetXPos() float64                    { return s.XPos }
func (s *MapStreamConfig) GetYPos() float64                    { return s.YPos }
func (s *MapStreamConfig) GetProperty(name string) interface{} { return s.Properties[name] }

// FilterStreamConfig is the configuration for the Filter transformation.
type FilterStreamConfig struct {
	ID                       int                    `yaml:"id" mapstructure:"id"`
	Name                     string                 `yaml:"name" mapstructure:"name"`
	Pipeline                 string                 `yaml:"pipeline" mapstructure:"pipeline"`
	IdService                int                    `yaml:"idService" mapstructure:"idService"`
	IdSource                 int                    `yaml:"idSource" mapstructure:"idSource"`
	XPos                     float64                `yaml:"xPos" mapstructure:"xPos"`
	YPos                     float64                `yaml:"yPos" mapstructure:"yPos"`
	FunctionPackage          string                 `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	FunctionName             string                 `yaml:"functionName,omitempty" mapstructure:"functionName"`
	PublicFunction           bool                   `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	FunctionDescription      string                 `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup string                 `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	FunctionModule           string                 `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	Properties               map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *FilterStreamConfig) GetID() int                          { return s.ID }
func (s *FilterStreamConfig) GetName() string                     { return s.Name }
func (s *FilterStreamConfig) GetPipeline() string                 { return s.Pipeline }
func (s *FilterStreamConfig) GetType() api.TransformationType     { return api.TransformationTypeFilter }
func (s *FilterStreamConfig) GetIdService() int                   { return s.IdService }
func (s *FilterStreamConfig) GetIdSource() int                    { return s.IdSource }
func (s *FilterStreamConfig) GetIdSources() []int                 { return nil }
func (s *FilterStreamConfig) GetXPos() float64                    { return s.XPos }
func (s *FilterStreamConfig) GetYPos() float64                    { return s.YPos }
func (s *FilterStreamConfig) GetProperty(name string) interface{} { return s.Properties[name] }

// JoinStreamConfig is the configuration for the Join transformation.
type JoinStreamConfig struct {
	ID                       int                    `yaml:"id" mapstructure:"id"`
	Name                     string                 `yaml:"name" mapstructure:"name"`
	Pipeline                 string                 `yaml:"pipeline" mapstructure:"pipeline"`
	IdService                int                    `yaml:"idService" mapstructure:"idService"`
	IdSource                 int                    `yaml:"idSource" mapstructure:"idSource"`
	IdSources                []int                  `yaml:"idSources" mapstructure:"idSources"`
	XPos                     float64                `yaml:"xPos" mapstructure:"xPos"`
	YPos                     float64                `yaml:"yPos" mapstructure:"yPos"`
	ValueType                string                 `yaml:"valueType,omitempty" mapstructure:"valueType"`
	JoinType                 api.JoinType           `yaml:"joinType,omitempty" mapstructure:"joinType"`
	JoinStorage              api.JoinStorageType    `yaml:"joinStorage,omitempty" mapstructure:"joinStorage"`
	Ttl                      int                    `yaml:"ttl,omitempty" mapstructure:"ttl"`
	RenewTTL                 bool                   `yaml:"renewTTL,omitempty" mapstructure:"renewTTL"`
	FunctionPackage          string                 `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	FunctionName             string                 `yaml:"functionName,omitempty" mapstructure:"functionName"`
	PublicFunction           bool                   `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	FunctionDescription      string                 `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup string                 `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	FunctionModule           string                 `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	Properties               map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *JoinStreamConfig) GetID() int                          { return s.ID }
func (s *JoinStreamConfig) GetName() string                     { return s.Name }
func (s *JoinStreamConfig) GetPipeline() string                 { return s.Pipeline }
func (s *JoinStreamConfig) GetType() api.TransformationType     { return api.TransformationTypeJoin }
func (s *JoinStreamConfig) GetIdService() int                   { return s.IdService }
func (s *JoinStreamConfig) GetIdSource() int                    { return s.IdSource }
func (s *JoinStreamConfig) GetIdSources() []int                 { return s.IdSources }
func (s *JoinStreamConfig) GetXPos() float64                    { return s.XPos }
func (s *JoinStreamConfig) GetYPos() float64                    { return s.YPos }
func (s *JoinStreamConfig) GetProperty(name string) interface{} { return s.Properties[name] }

// MultiJoinStreamConfig is the configuration for the MultiJoin transformation.
type MultiJoinStreamConfig struct {
	ID                       int                    `yaml:"id" mapstructure:"id"`
	Name                     string                 `yaml:"name" mapstructure:"name"`
	Pipeline                 string                 `yaml:"pipeline" mapstructure:"pipeline"`
	IdService                int                    `yaml:"idService" mapstructure:"idService"`
	IdSource                 int                    `yaml:"idSource" mapstructure:"idSource"`
	IdSources                []int                  `yaml:"idSources" mapstructure:"idSources"`
	XPos                     float64                `yaml:"xPos" mapstructure:"xPos"`
	YPos                     float64                `yaml:"yPos" mapstructure:"yPos"`
	ValueType                string                 `yaml:"valueType,omitempty" mapstructure:"valueType"`
	JoinStorage              api.JoinStorageType    `yaml:"joinStorage,omitempty" mapstructure:"joinStorage"`
	Ttl                      int                    `yaml:"ttl,omitempty" mapstructure:"ttl"`
	RenewTTL                 bool                   `yaml:"renewTTL,omitempty" mapstructure:"renewTTL"`
	FunctionPackage          string                 `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	FunctionName             string                 `yaml:"functionName,omitempty" mapstructure:"functionName"`
	PublicFunction           bool                   `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	FunctionDescription      string                 `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup string                 `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	FunctionModule           string                 `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	Properties               map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *MultiJoinStreamConfig) GetID() int          { return s.ID }
func (s *MultiJoinStreamConfig) GetName() string     { return s.Name }
func (s *MultiJoinStreamConfig) GetPipeline() string { return s.Pipeline }
func (s *MultiJoinStreamConfig) GetType() api.TransformationType {
	return api.TransformationTypeMultiJoin
}
func (s *MultiJoinStreamConfig) GetIdService() int                   { return s.IdService }
func (s *MultiJoinStreamConfig) GetIdSource() int                    { return s.IdSource }
func (s *MultiJoinStreamConfig) GetIdSources() []int                 { return s.IdSources }
func (s *MultiJoinStreamConfig) GetXPos() float64                    { return s.XPos }
func (s *MultiJoinStreamConfig) GetYPos() float64                    { return s.YPos }
func (s *MultiJoinStreamConfig) GetProperty(name string) interface{} { return s.Properties[name] }

// ProcessStreamConfig is the configuration for the Process transformation.
type ProcessStreamConfig struct {
	ID                       int                    `yaml:"id" mapstructure:"id"`
	Name                     string                 `yaml:"name" mapstructure:"name"`
	Pipeline                 string                 `yaml:"pipeline" mapstructure:"pipeline"`
	IdService                int                    `yaml:"idService" mapstructure:"idService"`
	IdSource                 int                    `yaml:"idSource" mapstructure:"idSource"`
	XPos                     float64                `yaml:"xPos" mapstructure:"xPos"`
	YPos                     float64                `yaml:"yPos" mapstructure:"yPos"`
	Pattern                  api.ProcessPattern     `yaml:"pattern,omitempty" mapstructure:"pattern"`
	FunctionPackage          string                 `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	FunctionName             string                 `yaml:"functionName,omitempty" mapstructure:"functionName"`
	PublicFunction           bool                   `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	FunctionDescription      string                 `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup string                 `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	FunctionModule           string                 `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	Properties               map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *ProcessStreamConfig) GetID() int                          { return s.ID }
func (s *ProcessStreamConfig) GetName() string                     { return s.Name }
func (s *ProcessStreamConfig) GetPipeline() string                 { return s.Pipeline }
func (s *ProcessStreamConfig) GetType() api.TransformationType     { return api.TransformationTypeProcess }
func (s *ProcessStreamConfig) GetIdService() int                   { return s.IdService }
func (s *ProcessStreamConfig) GetIdSource() int                    { return s.IdSource }
func (s *ProcessStreamConfig) GetIdSources() []int                 { return nil }
func (s *ProcessStreamConfig) GetXPos() float64                    { return s.XPos }
func (s *ProcessStreamConfig) GetYPos() float64                    { return s.YPos }
func (s *ProcessStreamConfig) GetProperty(name string) interface{} { return s.Properties[name] }

// DelayStreamConfig is the configuration for the Delay transformation.
type DelayStreamConfig struct {
	ID                       int                    `yaml:"id" mapstructure:"id"`
	Name                     string                 `yaml:"name" mapstructure:"name"`
	Pipeline                 string                 `yaml:"pipeline" mapstructure:"pipeline"`
	IdService                int                    `yaml:"idService" mapstructure:"idService"`
	IdSource                 int                    `yaml:"idSource" mapstructure:"idSource"`
	XPos                     float64                `yaml:"xPos" mapstructure:"xPos"`
	YPos                     float64                `yaml:"yPos" mapstructure:"yPos"`
	Duration                 int                    `yaml:"duration,omitempty" mapstructure:"duration"`
	FunctionPackage          string                 `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	FunctionName             string                 `yaml:"functionName,omitempty" mapstructure:"functionName"`
	PublicFunction           bool                   `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	FunctionDescription      string                 `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup string                 `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	FunctionModule           string                 `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	Properties               map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *DelayStreamConfig) GetID() int                          { return s.ID }
func (s *DelayStreamConfig) GetName() string                     { return s.Name }
func (s *DelayStreamConfig) GetPipeline() string                 { return s.Pipeline }
func (s *DelayStreamConfig) GetType() api.TransformationType     { return api.TransformationTypeDelay }
func (s *DelayStreamConfig) GetIdService() int                   { return s.IdService }
func (s *DelayStreamConfig) GetIdSource() int                    { return s.IdSource }
func (s *DelayStreamConfig) GetIdSources() []int                 { return nil }
func (s *DelayStreamConfig) GetXPos() float64                    { return s.XPos }
func (s *DelayStreamConfig) GetYPos() float64                    { return s.YPos }
func (s *DelayStreamConfig) GetProperty(name string) interface{} { return s.Properties[name] }

// FlatMapStreamConfig is the configuration for the FlatMap transformation.
type FlatMapStreamConfig struct {
	ID                       int                    `yaml:"id" mapstructure:"id"`
	Name                     string                 `yaml:"name" mapstructure:"name"`
	Pipeline                 string                 `yaml:"pipeline" mapstructure:"pipeline"`
	IdService                int                    `yaml:"idService" mapstructure:"idService"`
	IdSource                 int                    `yaml:"idSource" mapstructure:"idSource"`
	XPos                     float64                `yaml:"xPos" mapstructure:"xPos"`
	YPos                     float64                `yaml:"yPos" mapstructure:"yPos"`
	ValueType                string                 `yaml:"valueType,omitempty" mapstructure:"valueType"`
	FunctionPackage          string                 `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	FunctionName             string                 `yaml:"functionName,omitempty" mapstructure:"functionName"`
	PublicFunction           bool                   `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	FunctionDescription      string                 `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup string                 `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	FunctionModule           string                 `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	Properties               map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *FlatMapStreamConfig) GetID() int                          { return s.ID }
func (s *FlatMapStreamConfig) GetName() string                     { return s.Name }
func (s *FlatMapStreamConfig) GetPipeline() string                 { return s.Pipeline }
func (s *FlatMapStreamConfig) GetType() api.TransformationType     { return api.TransformationTypeFlatMap }
func (s *FlatMapStreamConfig) GetIdService() int                   { return s.IdService }
func (s *FlatMapStreamConfig) GetIdSource() int                    { return s.IdSource }
func (s *FlatMapStreamConfig) GetIdSources() []int                 { return nil }
func (s *FlatMapStreamConfig) GetXPos() float64                    { return s.XPos }
func (s *FlatMapStreamConfig) GetYPos() float64                    { return s.YPos }
func (s *FlatMapStreamConfig) GetProperty(name string) interface{} { return s.Properties[name] }

// FlatMapIterableStreamConfig is the configuration for the FlatMapIterable transformation.
type FlatMapIterableStreamConfig struct {
	ID         int                    `yaml:"id" mapstructure:"id"`
	Name       string                 `yaml:"name" mapstructure:"name"`
	Pipeline   string                 `yaml:"pipeline" mapstructure:"pipeline"`
	IdService  int                    `yaml:"idService" mapstructure:"idService"`
	IdSource   int                    `yaml:"idSource" mapstructure:"idSource"`
	XPos       float64                `yaml:"xPos" mapstructure:"xPos"`
	YPos       float64                `yaml:"yPos" mapstructure:"yPos"`
	ValueType  string                 `yaml:"valueType,omitempty" mapstructure:"valueType"`
	Properties map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *FlatMapIterableStreamConfig) GetID() int          { return s.ID }
func (s *FlatMapIterableStreamConfig) GetName() string     { return s.Name }
func (s *FlatMapIterableStreamConfig) GetPipeline() string { return s.Pipeline }
func (s *FlatMapIterableStreamConfig) GetType() api.TransformationType {
	return api.TransformationTypeFlatMapIterable
}
func (s *FlatMapIterableStreamConfig) GetIdService() int                   { return s.IdService }
func (s *FlatMapIterableStreamConfig) GetIdSource() int                    { return s.IdSource }
func (s *FlatMapIterableStreamConfig) GetIdSources() []int                 { return nil }
func (s *FlatMapIterableStreamConfig) GetXPos() float64                    { return s.XPos }
func (s *FlatMapIterableStreamConfig) GetYPos() float64                    { return s.YPos }
func (s *FlatMapIterableStreamConfig) GetProperty(name string) interface{} { return s.Properties[name] }

// KeyByStreamConfig is the configuration for the KeyBy transformation.
type KeyByStreamConfig struct {
	ID                       int                    `yaml:"id" mapstructure:"id"`
	Name                     string                 `yaml:"name" mapstructure:"name"`
	Pipeline                 string                 `yaml:"pipeline" mapstructure:"pipeline"`
	IdService                int                    `yaml:"idService" mapstructure:"idService"`
	IdSource                 int                    `yaml:"idSource" mapstructure:"idSource"`
	XPos                     float64                `yaml:"xPos" mapstructure:"xPos"`
	YPos                     float64                `yaml:"yPos" mapstructure:"yPos"`
	KeyType                  string                 `yaml:"keyType,omitempty" mapstructure:"keyType"`
	ValueType                string                 `yaml:"valueType,omitempty" mapstructure:"valueType"`
	FunctionPackage          string                 `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	FunctionName             string                 `yaml:"functionName,omitempty" mapstructure:"functionName"`
	PublicFunction           bool                   `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	FunctionDescription      string                 `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup string                 `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	FunctionModule           string                 `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	Properties               map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *KeyByStreamConfig) GetID() int                          { return s.ID }
func (s *KeyByStreamConfig) GetName() string                     { return s.Name }
func (s *KeyByStreamConfig) GetPipeline() string                 { return s.Pipeline }
func (s *KeyByStreamConfig) GetType() api.TransformationType     { return api.TransformationTypeKeyBy }
func (s *KeyByStreamConfig) GetIdService() int                   { return s.IdService }
func (s *KeyByStreamConfig) GetIdSource() int                    { return s.IdSource }
func (s *KeyByStreamConfig) GetIdSources() []int                 { return nil }
func (s *KeyByStreamConfig) GetXPos() float64                    { return s.XPos }
func (s *KeyByStreamConfig) GetYPos() float64                    { return s.YPos }
func (s *KeyByStreamConfig) GetProperty(name string) interface{} { return s.Properties[name] }

// MergeStreamConfig is the configuration for the Merge transformation.
type MergeStreamConfig struct {
	ID         int                    `yaml:"id" mapstructure:"id"`
	Name       string                 `yaml:"name" mapstructure:"name"`
	Pipeline   string                 `yaml:"pipeline" mapstructure:"pipeline"`
	IdService  int                    `yaml:"idService" mapstructure:"idService"`
	IdSources  []int                  `yaml:"idSources" mapstructure:"idSources"`
	XPos       float64                `yaml:"xPos" mapstructure:"xPos"`
	YPos       float64                `yaml:"yPos" mapstructure:"yPos"`
	Properties map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *MergeStreamConfig) GetID() int                          { return s.ID }
func (s *MergeStreamConfig) GetName() string                     { return s.Name }
func (s *MergeStreamConfig) GetPipeline() string                 { return s.Pipeline }
func (s *MergeStreamConfig) GetType() api.TransformationType     { return api.TransformationTypeMerge }
func (s *MergeStreamConfig) GetIdService() int                   { return s.IdService }
func (s *MergeStreamConfig) GetIdSource() int                    { return 0 }
func (s *MergeStreamConfig) GetIdSources() []int                 { return s.IdSources }
func (s *MergeStreamConfig) GetXPos() float64                    { return s.XPos }
func (s *MergeStreamConfig) GetYPos() float64                    { return s.YPos }
func (s *MergeStreamConfig) GetProperty(name string) interface{} { return s.Properties[name] }

// SplitStreamConfig is the configuration for the Split transformation.
type SplitStreamConfig struct {
	ID         int                    `yaml:"id" mapstructure:"id"`
	Name       string                 `yaml:"name" mapstructure:"name"`
	Pipeline   string                 `yaml:"pipeline" mapstructure:"pipeline"`
	IdService  int                    `yaml:"idService" mapstructure:"idService"`
	IdSource   int                    `yaml:"idSource" mapstructure:"idSource"`
	XPos       float64                `yaml:"xPos" mapstructure:"xPos"`
	YPos       float64                `yaml:"yPos" mapstructure:"yPos"`
	Properties map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *SplitStreamConfig) GetID() int                          { return s.ID }
func (s *SplitStreamConfig) GetName() string                     { return s.Name }
func (s *SplitStreamConfig) GetPipeline() string                 { return s.Pipeline }
func (s *SplitStreamConfig) GetType() api.TransformationType     { return api.TransformationTypeSplit }
func (s *SplitStreamConfig) GetIdService() int                   { return s.IdService }
func (s *SplitStreamConfig) GetIdSource() int                    { return s.IdSource }
func (s *SplitStreamConfig) GetIdSources() []int                 { return nil }
func (s *SplitStreamConfig) GetXPos() float64                    { return s.XPos }
func (s *SplitStreamConfig) GetYPos() float64                    { return s.YPos }
func (s *SplitStreamConfig) GetProperty(name string) interface{} { return s.Properties[name] }

// CaseStreamConfig is the configuration for the Case transformation.
type CaseStreamConfig struct {
	ID                       int                    `yaml:"id" mapstructure:"id"`
	Name                     string                 `yaml:"name" mapstructure:"name"`
	Pipeline                 string                 `yaml:"pipeline" mapstructure:"pipeline"`
	IdService                int                    `yaml:"idService" mapstructure:"idService"`
	IdSource                 int                    `yaml:"idSource" mapstructure:"idSource"`
	XPos                     float64                `yaml:"xPos" mapstructure:"xPos"`
	YPos                     float64                `yaml:"yPos" mapstructure:"yPos"`
	FunctionPackage          string                 `yaml:"functionPackage,omitempty" mapstructure:"functionPackage"`
	FunctionName             string                 `yaml:"functionName,omitempty" mapstructure:"functionName"`
	PublicFunction           bool                   `yaml:"publicFunction,omitempty" mapstructure:"publicFunction"`
	FunctionDescription      string                 `yaml:"functionDescription,omitempty" mapstructure:"functionDescription"`
	FunctionInitializerGroup string                 `yaml:"functionInitializerGroup,omitempty" mapstructure:"functionInitializerGroup"`
	FunctionModule           string                 `yaml:"functionModule,omitempty" mapstructure:"functionModule"`
	Properties               map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *CaseStreamConfig) GetID() int          { return s.ID }
func (s *CaseStreamConfig) GetName() string     { return s.Name }
func (s *CaseStreamConfig) GetPipeline() string { return s.Pipeline }
func (s *CaseStreamConfig) GetType() api.TransformationType {
	return api.TransformationTypeCase
}
func (s *CaseStreamConfig) GetIdService() int                   { return s.IdService }
func (s *CaseStreamConfig) GetIdSource() int                    { return s.IdSource }
func (s *CaseStreamConfig) GetIdSources() []int                 { return nil }
func (s *CaseStreamConfig) GetXPos() float64                    { return s.XPos }
func (s *CaseStreamConfig) GetYPos() float64                    { return s.YPos }
func (s *CaseStreamConfig) GetProperty(name string) interface{} { return s.Properties[name] }

// SinkStreamConfig is the configuration for the Sink transformation.
type SinkStreamConfig struct {
	ID         int                    `yaml:"id" mapstructure:"id"`
	Name       string                 `yaml:"name" mapstructure:"name"`
	Pipeline   string                 `yaml:"pipeline" mapstructure:"pipeline"`
	IdService  int                    `yaml:"idService" mapstructure:"idService"`
	IdSource   int                    `yaml:"idSource" mapstructure:"idSource"`
	XPos       float64                `yaml:"xPos" mapstructure:"xPos"`
	YPos       float64                `yaml:"yPos" mapstructure:"yPos"`
	IdEndpoint int                    `yaml:"idEndpoint,omitempty" mapstructure:"idEndpoint"`
	ValueType  string                 `yaml:"valueType,omitempty" mapstructure:"valueType"`
	Properties map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *SinkStreamConfig) GetID() int                          { return s.ID }
func (s *SinkStreamConfig) GetName() string                     { return s.Name }
func (s *SinkStreamConfig) GetPipeline() string                 { return s.Pipeline }
func (s *SinkStreamConfig) GetType() api.TransformationType     { return api.TransformationTypeSink }
func (s *SinkStreamConfig) GetIdService() int                   { return s.IdService }
func (s *SinkStreamConfig) GetIdSource() int                    { return s.IdSource }
func (s *SinkStreamConfig) GetIdSources() []int                 { return nil }
func (s *SinkStreamConfig) GetXPos() float64                    { return s.XPos }
func (s *SinkStreamConfig) GetYPos() float64                    { return s.YPos }
func (s *SinkStreamConfig) GetProperty(name string) interface{} { return s.Properties[name] }

// CycleLinkStreamConfig is the configuration for the CycleLink transformation.
type CycleLinkStreamConfig struct {
	ID         int                    `yaml:"id" mapstructure:"id"`
	Name       string                 `yaml:"name" mapstructure:"name"`
	Pipeline   string                 `yaml:"pipeline" mapstructure:"pipeline"`
	IdService  int                    `yaml:"idService" mapstructure:"idService"`
	IdSource   int                    `yaml:"idSource" mapstructure:"idSource"`
	XPos       float64                `yaml:"xPos" mapstructure:"xPos"`
	YPos       float64                `yaml:"yPos" mapstructure:"yPos"`
	Properties map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *CycleLinkStreamConfig) GetID() int          { return s.ID }
func (s *CycleLinkStreamConfig) GetName() string     { return s.Name }
func (s *CycleLinkStreamConfig) GetPipeline() string { return s.Pipeline }
func (s *CycleLinkStreamConfig) GetType() api.TransformationType {
	return api.TransformationTypeCycleLink
}
func (s *CycleLinkStreamConfig) GetIdService() int                   { return s.IdService }
func (s *CycleLinkStreamConfig) GetIdSource() int                    { return s.IdSource }
func (s *CycleLinkStreamConfig) GetIdSources() []int                 { return nil }
func (s *CycleLinkStreamConfig) GetXPos() float64                    { return s.XPos }
func (s *CycleLinkStreamConfig) GetYPos() float64                    { return s.YPos }
func (s *CycleLinkStreamConfig) GetProperty(name string) interface{} { return s.Properties[name] }

// WhenStreamConfig is the configuration for the Case transformation.
type WhenStreamConfig struct {
	ID         int                    `yaml:"id" mapstructure:"id"`
	Name       string                 `yaml:"name" mapstructure:"name"`
	Pipeline   string                 `yaml:"pipeline" mapstructure:"pipeline"`
	IdService  int                    `yaml:"idService" mapstructure:"idService"`
	IdSource   int                    `yaml:"idSource" mapstructure:"idSource"`
	XPos       float64                `yaml:"xPos" mapstructure:"xPos"`
	YPos       float64                `yaml:"yPos" mapstructure:"yPos"`
	ValueType  string                 `yaml:"valueType,omitempty" mapstructure:"valueType"`
	Properties map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (s *WhenStreamConfig) GetID() int          { return s.ID }
func (s *WhenStreamConfig) GetName() string     { return s.Name }
func (s *WhenStreamConfig) GetPipeline() string { return s.Pipeline }
func (s *WhenStreamConfig) GetType() api.TransformationType {
	return api.TransformationTypeWhen
}
func (s *WhenStreamConfig) GetIdService() int   { return s.IdService }
func (s *WhenStreamConfig) GetIdSource() int    { return s.IdSource }
func (s *WhenStreamConfig) GetIdSources() []int { return nil }
func (s *WhenStreamConfig) GetXPos() float64    { return s.XPos }
func (s *WhenStreamConfig) GetYPos() float64    { return s.YPos }
func (s *WhenStreamConfig) GetProperty(
	name string) interface{} {
	return s.Properties[name]
}
