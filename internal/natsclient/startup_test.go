package natsclient

import (
	"context"
	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
	"os"
	"testing"
	"time"
)

func TestStartupGate(t *testing.T) {
	url := os.Getenv("NATS_STARTUP_URL")
	if url == "" {
		t.Skip("native startup proxy required")
	}
	mode := os.Getenv("NATS_STARTUP_MODE")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if mode == "deadline" {
		var end context.CancelFunc
		ctx, end = context.WithTimeout(ctx, 100*time.Millisecond)
		defer end()
	}
	if mode == "pre-cancel" {
		cancel()
	}
	if mode == "cancel" {
		timer := time.AfterFunc(40*time.Millisecond, cancel)
		defer timer.Stop()
	}
	timeout := 1200
	if mode == "configured-timeout" {
		timeout = 100
	}
	begin := time.Now()
	conn, err := Connect(ctx, &config.NATSDataConnectorConfig{Type: api.DataConnectorTypeNATS, Servers: []string{url}, ConnectTimeoutMs: timeout}, func(error) {})
	if conn != nil {
		defer conn.Close()
	}
	if (err != nil) != (mode != "control") {
		t.Fatalf("unexpected startup outcome: %v", err)
	}
	if err != nil && conn != nil {
		t.Fatal("failed startup returned live handle")
	}
	if time.Since(begin) > 700*time.Millisecond {
		t.Fatal("startup exceeded focused gate bound")
	}
}
