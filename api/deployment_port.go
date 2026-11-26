package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"regexp"
)

// DeploymentPortValue explicitly represents a numeric or named container port.
// It accepts no arbitrary JSON and uses receiver-independent API wire scalars.
type DeploymentPortValue struct {
	number int
	name   string
}

// Equivalent to Kubernetes IsValidPortName: up to 15 lowercase alphanumeric
// characters/single hyphens, at least one letter, no leading/trailing hyphen.
var deploymentPortName = regexp.MustCompile(`^([0-9]+-)*[0-9]*[a-z][a-z0-9]*(-[a-z0-9]+)*$`)
var errDeploymentPort = errors.New("deployment port must be a number from 1 to 65535 or a valid named port")

func NewDeploymentPortNumber(number int) (DeploymentPortValue, error) {
	if number < 1 || number > 65535 {
		return DeploymentPortValue{}, errDeploymentPort
	}
	return DeploymentPortValue{number: number}, nil
}

func NewDeploymentPortName(name string) (DeploymentPortValue, error) {
	if len(name) > 15 || !deploymentPortName.MatchString(name) {
		return DeploymentPortValue{}, errDeploymentPort
	}
	return DeploymentPortValue{name: name}, nil
}

func (v DeploymentPortValue) Number() (int, bool)  { return v.number, v.name == "" && v.number > 0 }
func (v DeploymentPortValue) Name() (string, bool) { return v.name, v.name != "" }

func (v DeploymentPortValue) MarshalJSON() ([]byte, error) {
	if v.name != "" {
		return json.Marshal(v.name)
	}
	if v.number < 1 || v.number > 65535 {
		return nil, errDeploymentPort
	}
	return json.Marshal(v.number)
}

func (v *DeploymentPortValue) UnmarshalJSON(data []byte) error {
	if v == nil {
		return errDeploymentPort
	}
	data = bytes.TrimSpace(data)
	var next DeploymentPortValue
	var err error
	if len(data) > 0 && data[0] == '"' {
		var name string
		if json.Unmarshal(data, &name) != nil {
			return errDeploymentPort
		}
		next, err = NewDeploymentPortName(name)
	} else {
		var number int
		if bytes.Equal(data, []byte("null")) || json.Unmarshal(data, &number) != nil {
			return errDeploymentPort
		}
		next, err = NewDeploymentPortNumber(number)
	}
	if err != nil {
		return err
	}
	*v = next
	return nil
}
