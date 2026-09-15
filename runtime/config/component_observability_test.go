package config

import (
	"testing"

	"github.com/gorundebug/servicelib/api"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestComponentDefinitionSurvivesRuntimeAPIAndYAML(t *testing.T) {
	for _, name := range []string{"CustomerPricing", ""} {
		t.Run(name, func(t *testing.T) {
			stream := StreamConfigToAPI(&MapStreamConfig{
				ID: 7, IdService: 1, Name: "CalculatePrice", Pipeline: "reserve", Component: name,
			})
			require.Equal(t, "reserve", *stream.Pipeline)
			if name == "" {
				require.Nil(t, stream.Component)
			} else {
				require.Equal(t, name, *stream.Component)
			}
			data, err := AppToYaml(&api.StreamApp{
				Services: []api.Service{{Id: 1, Name: "Booking"}}, Streams: []api.Stream{stream},
			})
			require.NoError(t, err)
			var document map[string]any
			require.NoError(t, yaml.Unmarshal(data, &document))
			service := document["services"].(map[string]any)["booking"].(map[string]any)
			written := service["pipelines"].(map[string]any)["reserve"].(map[string]any)["calculatePrice"].(map[string]any)
			if name == "" {
				require.NotContains(t, written, "component")
			} else {
				require.Equal(t, name, written["component"])
			}
			require.NotContains(t, written, "component_instance")
		})
	}
}
