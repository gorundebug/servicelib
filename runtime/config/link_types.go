/*
 * Copyright (c) 2026 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package config

import (
	"fmt"

	"github.com/gorundebug/servicelib/api"
)

type CallSemanticsConfig interface {
	GetType() api.CallSemantics
}

type FunctionCallSemanticsConfig struct {
}

func (f *FunctionCallSemanticsConfig) GetType() api.CallSemantics {
	return api.CallSemanticsFunctionCall
}

type TaskPoolCallSemanticsConfig struct {
	PoolName string `yaml:"poolName" mapstructure:"poolName"`
}

func (*TaskPoolCallSemanticsConfig) GetType() api.CallSemantics {
	return api.CallSemanticsTaskPool
}

type PriorityTaskPoolCallSemanticsConfig struct {
	PoolName string `yaml:"poolName" mapstructure:"poolName"`
	Priority int    `yaml:"priority" mapstructure:"priority"`
}

func (*PriorityTaskPoolCallSemanticsConfig) GetType() api.CallSemantics {
	return api.CallSemanticsPriorityTaskPool
}

type CallSemanticsGroup struct {
	FunctionCall     *FunctionCallSemanticsConfig         `yaml:"function,omitempty" mapstructure:"functionCall"`
	TaskPool         *TaskPoolCallSemanticsConfig         `yaml:"taskPool,omitempty" mapstructure:"taskPool"`
	PriorityTaskPool *PriorityTaskPoolCallSemanticsConfig `yaml:"priorityTaskPool,omitempty" mapstructure:"priorityTaskPool"`
}

func (g *CallSemanticsGroup) Get() CallSemanticsConfig {
	switch {
	case g.FunctionCall != nil:
		return g.FunctionCall
	case g.TaskPool != nil:
		return g.TaskPool
	case g.PriorityTaskPool != nil:
		return g.PriorityTaskPool
	}
	return nil
}

func (g *CallSemanticsGroup) Validate() error {
	c := 0
	if g.FunctionCall != nil {
		c++
	}
	if g.TaskPool != nil {
		c++
	}
	if g.PriorityTaskPool != nil {
		c++
	}
	if c != 1 {
		return fmt.Errorf("exactly one call semantics must be specified, got %d", c)
	}
	return nil
}

// LinkConfig - конфигурация для link
type LinkConfig struct {
	From          int                    `yaml:"from" mapstructure:"from"`
	To            int                    `yaml:"to" mapstructure:"to"`
	CallSemantics *CallSemanticsGroup    `yaml:"callSemantics,omitempty" mapstructure:"callSemantics"`
	Properties    map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

func (l *LinkConfig) GetCallSemantics() CallSemanticsConfig {
	if l.CallSemantics == nil {
		return nil
	}
	return l.CallSemantics.Get()
}

func (l *LinkConfig) GetProperty(name string) interface{} { return l.Properties[name] }

func (l *LinkConfig) Validate() error {
	if l.CallSemantics == nil {
		return nil
	}
	if err := l.CallSemantics.Validate(); err != nil {
		return fmt.Errorf("link From: %d, To: %d has invalid configuration: %w", l.From, l.To, err)
	}
	return nil
}
