// Package pulsarclient owns Apache Pulsar native clients and startup provisioning.
package pulsarclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
)

type admin struct {
	client      *http.Client
	base, token string
}

type adminError struct{ status int }

func (e adminError) Error() string {
	return fmt.Sprintf("Pulsar administration returned HTTP %d", e.status)
}
func statusIs(err error, status int) bool {
	var e adminError
	return errors.As(err, &e) && e.status == status
}

func newAdmin(c *config.PulsarDataConnectorConfig) (*admin, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.TLSCAFile != "" {
		pem, err := os.ReadFile(c.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("read Pulsar admin CA: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("Pulsar admin CA contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	if c.TLSCertFile != "" {
		cert, err := tls.LoadX509KeyPair(c.TLSCertFile, c.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load Pulsar admin TLS identity: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: c.ConnectTimeout()}).DialContext
	transport.TLSClientConfig = tlsConfig
	transport.TLSHandshakeTimeout = c.ConnectTimeout()
	transport.ResponseHeaderTimeout = c.OperationTimeout()
	client := &http.Client{Transport: transport, Timeout: c.OperationTimeout(), CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return errors.New("too many Pulsar admin redirects")
		}
		if req.URL.Host != via[0].URL.Host || req.URL.Scheme != via[0].URL.Scheme {
			return errors.New("Pulsar admin redirect requires the configured origin")
		}
		return nil
	}}
	token := c.AdminToken
	if token == "" {
		token = c.Token
	}
	return &admin{client: client, base: c.AdminURL, token: token}, nil
}
func (a *admin) close() { a.client.CloseIdleConnections() }
func (a *admin) request(ctx context.Context, method, path string, body interface{}, result interface{}) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, a.base+path, bytes.NewReader(data))
	if err != nil {
		return errors.New("invalid Pulsar administration request")
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	response, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("Pulsar administration transport: %w", err)
	}
	defer response.Body.Close()
	// Error bodies can contain credentials or broker internals; never copy them to errors.
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return adminError{response.StatusCode}
	}
	if result == nil {
		return nil
	}
	const limit = 4 << 20
	data, err = io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return err
	}
	if len(data) > limit {
		return errors.New("Pulsar administration response exceeds limit")
	}
	if err = json.Unmarshal(data, result); err != nil {
		return errors.New("invalid Pulsar administration JSON response")
	}
	return nil
}
func (a *admin) requireDisabled(ctx context.Context, ns, policy, field string) error {
	var result map[string]interface{}
	if err := a.request(ctx, http.MethodGet, ns+"/"+policy, nil, &result); err != nil {
		return err
	}
	allowed, ok := result[field].(bool)
	if !ok || allowed {
		return fmt.Errorf("Pulsar namespace must explicitly disable %s before endpoint startup", field)
	}
	return nil
}
func (a *admin) partitions(ctx context.Context, topic string) (int, error) {
	var result struct {
		Partitions *int `json:"partitions"`
	}
	if err := a.request(ctx, http.MethodGet, topic+"/partitions?checkAllowAutoCreation=false", nil, &result); err != nil {
		return 0, err
	}
	if result.Partitions == nil || *result.Partitions < 0 {
		return 0, errors.New("invalid Pulsar partition metadata")
	}
	if *result.Partitions == 0 {
		// Metadata 0 alone does not distinguish a non-partitioned topic from absence.
		if err := a.request(ctx, http.MethodGet, topic+"/stats", nil, nil); err != nil {
			return 0, err
		}
	}
	return *result.Partitions, nil
}
func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

// EnsureResources does not mutate namespace policies or existing topic settings.
// Explicit namespace overrides prevent native clients from implicitly recreating
// resources between this check and connection. Policies must remain enforced by operators.
func EnsureResources(ctx context.Context, c *config.PulsarDataConnectorConfig, e *config.PulsarEndpointConfig, source bool) error {
	if err := e.Validate(source); err != nil {
		return err
	}
	a, err := newAdmin(c)
	if err != nil {
		return err
	}
	defer a.close()
	parts := strings.Split(strings.TrimPrefix(e.Topic, "persistent://"), "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	ns := "/admin/v2/namespaces/" + parts[0] + "/" + parts[1]
	topic := "/admin/v2/persistent/" + strings.Join(parts, "/")
	if err = a.requireDisabled(ctx, ns, "autoTopicCreation", "allowAutoTopicCreation"); err != nil {
		return err
	}
	if source {
		if err = a.requireDisabled(ctx, ns, "autoSubscriptionCreation", "allowAutoSubscriptionCreation"); err != nil {
			return err
		}
	}
	partitions, err := a.partitions(ctx, topic)
	if statusIs(err, http.StatusNotFound) && e.CreateTopic {
		createPath := topic
		var body interface{}
		if e.Partitions > 0 {
			createPath += "/partitions"
			body = e.Partitions
		}
		err = a.request(ctx, http.MethodPut, createPath, body, nil)
		if err != nil && !statusIs(err, http.StatusConflict) {
			return err
		}
		// Re-read even after 409: never assume a concurrent creator chose our shape.
		partitions, err = a.partitions(ctx, topic)
	}
	if err != nil {
		return err
	}
	if e.CreateTopic && partitions != e.Partitions {
		return errors.New("existing Pulsar topic partition count differs; no reconciliation performed")
	}
	if !source {
		return nil
	}
	var subscriptions []string
	if err = a.request(ctx, http.MethodGet, topic+"/subscriptions", nil, &subscriptions); err != nil {
		return err
	}
	if contains(subscriptions, e.SubscriptionName) {
		return nil
	}
	if !e.CreateSubscription {
		return errors.New("Pulsar subscription does not exist and creation is disabled")
	}
	var position int64 = -1
	if e.InitialPosition() == api.Latest {
		position = math.MaxInt64
	}
	cursor := struct {
		LedgerID       int64 `json:"ledgerId"`
		EntryID        int64 `json:"entryId"`
		PartitionIndex int   `json:"partitionIndex"`
	}{position, position, -1}
	err = a.request(ctx, http.MethodPut, topic+"/subscription/"+url.PathEscape(e.SubscriptionName), cursor, nil)
	if err != nil && !statusIs(err, http.StatusConflict) {
		return err
	}
	subscriptions = nil
	if err = a.request(ctx, http.MethodGet, topic+"/subscriptions", nil, &subscriptions); err != nil {
		return err
	}
	if !contains(subscriptions, e.SubscriptionName) {
		return errors.New("Pulsar subscription was not visible after creation")
	}
	return nil
}
