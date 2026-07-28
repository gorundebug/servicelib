package runtime

import (
	"strings"
	"testing"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/stretchr/testify/require"
)

func TestStatusPageRefreshesLiveCounters(t *testing.T) {
	require.True(t, strings.Contains(string(statusHtml), "new vis.DataSet"))
	require.True(t, strings.Contains(
		string(statusHtml),
		"window.setTimeout(refreshNetwork, 1000)",
	))
}

func TestStatusIconsMatchGraphDesignerEndpointOverrides(t *testing.T) {
	runtimeConfig, err := config.NewRuntimeConfig(&testConnectorConfig{})
	require.NoError(t, err)

	input := &config.InputStreamConfig{IdEndpoint: 1}
	sink := &config.SinkStreamConfig{IdEndpoint: 1}

	require.Equal(t, mdiAPI, statusIconPath(runtimeConfig, input))
	require.Equal(t, mdiCallMade, statusIconPath(runtimeConfig, sink))
}
