package pulsarclient

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
)

func TestNativePulsarResourceProvisioning(t *testing.T) {
	adminURL := os.Getenv("PULSAR_TEST_ADMIN_URL")
	if adminURL == "" {
		t.Skip("set PULSAR_TEST_ADMIN_URL for isolated Pulsar")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	c := &config.PulsarDataConnectorConfig{Servers: []string{"pulsar://127.0.0.1:16650"}, AdminURL: adminURL, OperationTimeoutMs: 5000}
	a, err := newAdmin(c)
	require.NoError(t, err)
	defer a.close()
	namespace := "public/sa-" + xid.New().String()
	ns := "/admin/v2/namespaces/" + namespace
	require.NoError(t, a.request(ctx, http.MethodPut, ns, map[string]interface{}{}, nil))
	defer func() {
		for _, suffix := range []string{"/events-0", "/events-2/partitions"} {
			err := a.request(ctx, http.MethodDelete, "/admin/v2/persistent/"+namespace+suffix+"?force=true", nil, nil)
			if !statusIs(err, http.StatusNotFound) {
				require.NoError(t, err)
			}
		}
		require.NoError(t, a.request(ctx, http.MethodDelete, ns, nil, nil))
	}()
	require.NoError(t, a.request(ctx, http.MethodPost, ns+"/autoTopicCreation", map[string]interface{}{"allowAutoTopicCreation": false}, nil))
	require.NoError(t, a.request(ctx, http.MethodPost, ns+"/autoSubscriptionCreation", map[string]interface{}{"allowAutoSubscriptionCreation": false}, nil))
	for _, partitions := range []int{0, 2} {
		t.Run(fmt.Sprintf("partitions-%d", partitions), func(t *testing.T) {
			e := &config.PulsarEndpointConfig{Topic: fmt.Sprintf("persistent://%s/events-%d", namespace, partitions), SubscriptionName: "workers"}
			require.Error(t, EnsureResources(ctx, c, e, true))
			e.CreateTopic = true
			e.Partitions = partitions
			require.ErrorContains(t, EnsureResources(ctx, c, e, true), "subscription does not exist")
			e.CreateSubscription = true
			require.NoError(t, EnsureResources(ctx, c, e, true))
			require.NoError(t, EnsureResources(ctx, c, e, true))
			e.CreateTopic = false
			e.Partitions = 0
			e.CreateSubscription = false
			require.NoError(t, EnsureResources(ctx, c, e, true))
		})
	}
}
