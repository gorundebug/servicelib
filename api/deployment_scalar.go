package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"regexp"
)

// DeploymentCountOrPercent is the explicit integer-or-percentage API union.
// It cannot contain arbitrary JSON. The zero value represents count zero;
// optional containing fields use pointers to distinguish omission from zero.
type DeploymentCountOrPercent struct {
	count   int
	percent string
}

var deploymentPercent = regexp.MustCompile(`^[0-9]+%$`)
var errDeploymentCountOrPercent = errors.New("deployment count must be nonnegative or a percentage")

func NewDeploymentCount(count int) (DeploymentCountOrPercent, error) {
	if count < 0 {
		return DeploymentCountOrPercent{}, errDeploymentCountOrPercent
	}
	return DeploymentCountOrPercent{count: count}, nil
}

func NewDeploymentPercent(percent string) (DeploymentCountOrPercent, error) {
	if !deploymentPercent.MatchString(percent) {
		return DeploymentCountOrPercent{}, errDeploymentCountOrPercent
	}
	return DeploymentCountOrPercent{percent: percent}, nil
}

func (v DeploymentCountOrPercent) Count() (int, bool)      { return v.count, v.percent == "" }
func (v DeploymentCountOrPercent) Percent() (string, bool) { return v.percent, v.percent != "" }

func (v DeploymentCountOrPercent) MarshalJSON() ([]byte, error) {
	if v.percent != "" {
		return json.Marshal(v.percent)
	}
	return json.Marshal(v.count)
}

func (v *DeploymentCountOrPercent) UnmarshalJSON(data []byte) error {
	if v == nil {
		return errDeploymentCountOrPercent
	}
	data = bytes.TrimSpace(data)
	var next DeploymentCountOrPercent
	var err error
	if len(data) > 0 && data[0] == '"' {
		var percent string
		if json.Unmarshal(data, &percent) != nil {
			return errDeploymentCountOrPercent
		}
		next, err = NewDeploymentPercent(percent)
	} else {
		var count int
		if bytes.Equal(data, []byte("null")) || json.Unmarshal(data, &count) != nil {
			return errDeploymentCountOrPercent
		}
		next, err = NewDeploymentCount(count)
	}
	if err != nil {
		return err
	}
	*v = next
	return nil
}
