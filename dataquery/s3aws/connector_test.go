package s3aws

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
)

type testEnvironment struct {
	runtime.RuntimeEnvironment
	connectors map[int]runtime.ManagedDataConnector
}

type makerTransport struct{ closed int }

func (t *makerTransport) CloseIdleConnections() { t.closed++ }

func TestMakerSuppliedClientLifecycle(t *testing.T) {
	client := s3.NewFromConfig(aws.Config{Region: "fixture"})
	transport := &makerTransport{}
	cfg := &config.S3AWSDataConnectorConfig{ID: 9, Name: "custom"}
	if _, err := NewConnector(cfg, nil, nil); err == nil {
		t.Fatal("nil client accepted")
	}
	connector, err := NewConnector(cfg, client, transport)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := connector.Start(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("lost startup context: %v", err)
	}
	for i := 1; i <= 2; i++ {
		if err := connector.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		actual, err := connector.nativeClient()
		if err != nil || actual != client {
			t.Fatalf("maker client replaced: %v", err)
		}
		connector.Stop(t.Context())
		connector.Stop(t.Context())
		if transport.closed != i {
			t.Fatalf("transport cleanup count: %d", transport.closed)
		}
		if _, err := connector.nativeClient(); err == nil {
			t.Fatal("stopped connector accepts requests")
		}
	}
}

func (e *testEnvironment) GetManagedDataConnector(id int) runtime.ManagedDataConnector {
	return e.connectors[id]
}
func (e *testEnvironment) AddManagedDataConnector(c runtime.ManagedDataConnector) {
	e.connectors[c.GetID()] = c
}

func TestManagedConnectorLifecycle(t *testing.T) {
	// Isolate provider lookup from the developer's credentials and cloud metadata.
	t.Setenv("AWS_ACCESS_KEY_ID", "fixture-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "fixture-secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bucket/key" {
			t.Errorf("path-style address lost: %s", r.URL.Path)
		}
		w.Header().Set("Content-Length", "0")
	}))
	defer server.Close()
	env := &testEnvironment{connectors: map[int]runtime.ManagedDataConnector{}}
	cfg := &config.S3AWSDataConnectorConfig{ID: 1, Name: "objects", Region: "us-east-1", Address: server.URL, ForcePathStyle: true}
	c, err := MakeConnector(env, cfg)
	if err != nil {
		t.Fatal(err)
	}
	reused, err := MakeConnector(env, cfg)
	if err != nil || reused != c {
		t.Fatalf("reuse failed: %v", err)
	}
	request := &s3.HeadObjectInput{Bucket: aws.String("bucket"), Key: aws.String("key")}
	if _, err = c.HeadObject(t.Context(), request); err == nil {
		t.Fatal("operation before startup accepted")
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err = c.Start(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled startup: %v", err)
	}
	if err = c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	first := c.client
	if err = c.Start(t.Context()); err != nil || first != c.client {
		t.Fatal("repeated start replaced client")
	}
	if _, err = c.HeadObject(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	c.Stop(t.Context())
	if _, err = c.HeadObject(t.Context(), request); err == nil {
		t.Fatal("operation after stop accepted")
	}
	if c.transport != nil {
		t.Fatal("transport retained after stop")
	}
	document := config.DataConnectorConfigToAPI(cfg)
	if document.Implementations != nil || document.Region == nil || *document.Region != "us-east-1" || document.Address != nil {
		t.Fatalf("unexpected public configuration: %+v", document)
	}
}
func TestRejectsCredentialBearingEndpoint(t *testing.T) {
	for _, address := range []string{"https://user:private-secret@localhost", "https://localhost?token=private-secret", "https://localhost#private-secret", "file:///tmp/object"} {
		env := &testEnvironment{connectors: map[int]runtime.ManagedDataConnector{}}
		_, err := MakeConnector(env, &config.S3AWSDataConnectorConfig{ID: 1, Address: address})
		if err == nil || strings.Contains(err.Error(), "private-secret") {
			t.Fatalf("unsafe endpoint validation: %v", err)
		}
	}
}
