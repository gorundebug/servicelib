package pulsarclient

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/stretchr/testify/require"
)

func TestResourceProvisioningModes(t *testing.T) {
	for _, tc := range []struct {
		name                                                                 string
		exists, subscription, createTopic, createSub, unsafePolicy, conflict bool
		wantError                                                            bool
		writes                                                               int
	}{
		{name: "existing", exists: true, subscription: true},
		{name: "absent forbidden", wantError: true},
		{name: "subscription absent", exists: true, wantError: true},
		{name: "topic only", createTopic: true, wantError: true, writes: 1},
		{name: "explicit both", createTopic: true, createSub: true, writes: 2},
		{name: "concurrent creator", createTopic: true, createSub: true, conflict: true, writes: 2},
		{name: "unsafe policy", exists: true, subscription: true, unsafePolicy: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exists, sub := tc.exists, tc.subscription
			writes := 0
			topic := "/admin/v2/persistent/tenant/ns/events"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer private-token" {
					w.WriteHeader(401)
					return
				}
				if r.Method == http.MethodPut {
					writes++
				}
				switch {
				case strings.HasSuffix(r.URL.Path, "/autoTopicCreation"):
					require.Equal(t, http.MethodGet, r.Method)
					fmt.Fprintf(w, `{"allowAutoTopicCreation":%t}`, tc.unsafePolicy)
				case strings.HasSuffix(r.URL.Path, "/autoSubscriptionCreation"):
					require.Equal(t, http.MethodGet, r.Method)
					fmt.Fprint(w, `{"allowAutoSubscriptionCreation":false}`)
				case r.URL.Path == topic+"/partitions":
					require.Equal(t, "false", r.URL.Query().Get("checkAllowAutoCreation"))
					fmt.Fprint(w, `{"partitions":0}`)
				case r.URL.Path == topic+"/stats":
					if !exists {
						w.WriteHeader(404)
						return
					}
					fmt.Fprint(w, `{}`)
				case r.URL.Path == topic && r.Method == http.MethodPut:
					exists = true
					if tc.conflict {
						w.WriteHeader(409)
					} else {
						w.WriteHeader(204)
					}
				case r.URL.Path == topic+"/subscriptions":
					if sub {
						fmt.Fprint(w, `["workers"]`)
					} else {
						fmt.Fprint(w, `[]`)
					}
				case r.URL.Path == topic+"/subscription/workers" && r.Method == http.MethodPut:
					var cursor struct {
						LedgerID int64 `json:"ledgerId"`
						EntryID  int64 `json:"entryId"`
					}
					require.NoError(t, json.NewDecoder(r.Body).Decode(&cursor))
					require.EqualValues(t, -1, cursor.LedgerID)
					require.EqualValues(t, -1, cursor.EntryID)
					sub = true
					if tc.conflict {
						w.WriteHeader(409)
					} else {
						w.WriteHeader(204)
					}
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL)
					w.WriteHeader(500)
				}
			}))
			defer server.Close()
			c := &config.PulsarDataConnectorConfig{Servers: []string{"pulsar://broker"}, AdminURL: server.URL, Token: "private-token"}
			e := &config.PulsarEndpointConfig{Topic: "persistent://tenant/ns/events", SubscriptionName: "workers", CreateTopic: tc.createTopic, CreateSubscription: tc.createSub}
			err := EnsureResources(t.Context(), c, e, true)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.writes, writes)
		})
	}
}
func TestAdminRejectsUnverifiedPoliciesAndRedactsErrors(t *testing.T) {
	for _, body := range []string{`null`, `{}`, `{"allowAutoTopicCreation":"false"}`, `{"allowAutoTopicCreation":true}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodGet, r.Method)
			fmt.Fprint(w, body)
		}))
		c := &config.PulsarDataConnectorConfig{Servers: []string{"pulsar://broker"}, AdminURL: server.URL}
		require.Error(t, EnsureResources(t.Context(), c, &config.PulsarEndpointConfig{Topic: "persistent://a/b/c"}, false))
		server.Close()
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(403); fmt.Fprint(w, "private-token") }))
	defer server.Close()
	c := &config.PulsarDataConnectorConfig{Servers: []string{"pulsar://broker"}, AdminURL: server.URL, Token: "private-token"}
	err := EnsureResources(t.Context(), c, &config.PulsarEndpointConfig{Topic: "persistent://a/b/c", CreateTopic: true}, false)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "private-token")
	require.True(t, statusIs(err, 403))
}
func TestAdminRejectsMismatchedPartitionShape(t *testing.T) {
	puts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			puts++
		}
		if strings.Contains(r.URL.Path, "autoTopicCreation") {
			fmt.Fprint(w, `{"allowAutoTopicCreation":false}`)
		} else {
			fmt.Fprint(w, `{"partitions":3}`)
		}
	}))
	defer server.Close()
	c := &config.PulsarDataConnectorConfig{Servers: []string{"pulsar://broker"}, AdminURL: server.URL}
	require.ErrorContains(t, EnsureResources(t.Context(), c, &config.PulsarEndpointConfig{Topic: "persistent://a/b/c", CreateTopic: true, Partitions: 2}, false), "partition count differs")
	require.Zero(t, puts)
}
func TestAdminCancellationAndCredentialRedirect(t *testing.T) {
	reached := false
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached = true; w.WriteHeader(204) }))
	defer foreign.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, foreign.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	c := &config.PulsarDataConnectorConfig{Servers: []string{"pulsar://broker"}, AdminURL: redirect.URL, Token: "private-token"}
	a, err := newAdmin(c)
	require.NoError(t, err)
	defer a.close()
	require.Error(t, a.request(t.Context(), http.MethodGet, "/", nil, nil))
	require.False(t, reached)
	stalled := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer stalled.Close()
	c.AdminURL = stalled.URL
	c.OperationTimeoutMs = 25
	slow, err := newAdmin(c)
	require.NoError(t, err)
	defer slow.close()
	start := time.Now()
	require.Error(t, slow.request(t.Context(), http.MethodGet, "/", nil, nil))
	require.Less(t, time.Since(start), time.Second)
}
