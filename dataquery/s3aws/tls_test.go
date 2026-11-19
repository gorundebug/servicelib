package s3aws

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
)

func TestNativeCABundleTLS(t *testing.T) {
	for key, value := range map[string]string{
		"AWS_ACCESS_KEY_ID": "fixture", "AWS_SECRET_ACCESS_KEY": "fixture", "AWS_SESSION_TOKEN": "",
		"AWS_PROFILE": "", "AWS_EC2_METADATA_DISABLED": "true", "AWS_CA_BUNDLE": "",
		"AWS_CONFIG_FILE":             filepath.Join(t.TempDir(), "config"),
		"AWS_SHARED_CREDENTIALS_FILE": filepath.Join(t.TempDir(), "credentials"),
	} {
		t.Setenv(key, value)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			t.Error("unencrypted request")
		}
		w.Header().Set("Content-Length", "0")
	}))
	defer server.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	for _, trusted := range []bool{false, true} {
		t.Run(map[bool]string{false: "reject-untrusted", true: "accept-configured-ca"}[trusted], func(t *testing.T) {
			if trusted {
				t.Setenv("AWS_CA_BUNDLE", ca)
			}
			env := &testEnvironment{connectors: map[int]runtime.ManagedDataConnector{}}
			c, err := MakeConnector(env, &config.S3AWSDataConnectorConfig{ID: 1, Region: "us-east-1", Address: server.URL, ForcePathStyle: true})
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			defer c.Stop(t.Context())
			client, err := c.nativeClient()
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.HeadObject(t.Context(), &s3.HeadObjectInput{Bucket: aws.String("bucket"), Key: aws.String("key")})
			if trusted && err != nil {
				t.Fatal(err)
			}
			if !trusted && err == nil {
				t.Fatal("untrusted certificate accepted")
			}
		})
	}
}
