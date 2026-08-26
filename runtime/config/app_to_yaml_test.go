package config

import (
	"testing"

	"github.com/gorundebug/servicelib/api"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestAppToYamlWritesVisualFieldsOnlyBelowAppearance(t *testing.T) {
	pipeline := "main"
	app := &api.StreamApp{
		Services: []api.Service{{
			Id: 1, Name: "Order Service", Color: "#123456",
		}},
		Streams: []api.Stream{{
			Id: 1, IdService: 1, Name: "Input", Pipeline: &pipeline,
			Type: api.TransformationTypeInput, XPos: 12.5, YPos: -7,
		}},
	}

	data, err := AppToYaml(app)
	require.NoError(t, err)
	var document map[string]any
	require.NoError(t, yaml.Unmarshal(data, &document))
	service := document["services"].(map[string]any)["orderService"].(map[string]any)
	require.NotContains(t, service, "color")
	stream := service["pipelines"].(map[string]any)["main"].(map[string]any)["input"].(map[string]any)
	require.NotContains(t, stream, "xPos")
	require.NotContains(t, stream, "yPos")
	appearance := service["appearance"].(map[string]any)
	require.Equal(t, "#123456", appearance["color"])
	position := appearance["pipelines"].(map[string]any)["main"].(map[string]any)["input"].(map[string]any)
	require.Equal(t, 12.5, position["x"])
	require.EqualValues(t, -7, position["y"])
}
