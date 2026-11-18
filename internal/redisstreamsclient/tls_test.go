package redisstreamsclient

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/redis/go-redis/v9"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
)

func TestNativeTLSAuthentication(t *testing.T) {
	address := os.Getenv("REDIS_STREAMS_TEST_URL")
	if address == "" {
		t.Skip("requires isolated Redis")
	}
	options, err := redis.ParseURL(address)
	require.NoError(t, err)
	options.Password = os.Getenv("REDIS_STREAMS_TEST_PASSWORD")
	admin := redis.NewClient(options)
	defer admin.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Redis test CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caPath, caPEM, 0600))
	issue := func(name string, serial int64, usage x509.ExtKeyUsage) (tls.Certificate, string, string) {
		key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, e)
		cert := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
		der, e := x509.CreateCertificate(rand.Reader, cert, ca, &key.PublicKey, caKey)
		require.NoError(t, e)
		rawKey, e := x509.MarshalPKCS8PrivateKey(key)
		require.NoError(t, e)
		cp, kp := filepath.Join(dir, name+".pem"), filepath.Join(dir, name+".key")
		require.NoError(t, os.WriteFile(cp, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600))
		require.NoError(t, os.WriteFile(kp, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: rawKey}), 0600))
		pair, e := tls.LoadX509KeyPair(cp, kp)
		require.NoError(t, e)
		return pair, cp, kp
	}
	server, _, _ := issue("redis.test", 2, x509.ExtKeyUsageServerAuth)
	_, clientCert, clientKey := issue("client.test", 3, x509.ExtKeyUsageClientAuth)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(caPEM))
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{server}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots})
	require.NoError(t, err)
	var workers sync.WaitGroup
	var connections sync.Map
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			client, e := listener.Accept()
			if e != nil {
				return
			}
			connections.Store(client, true)
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer client.Close()
				defer connections.Delete(client)
				_ = client.SetDeadline(time.Now().Add(10 * time.Second))
				if client.(*tls.Conn).Handshake() != nil {
					return
				}
				upstream, e := net.DialTimeout("tcp", options.Addr, time.Second)
				if e != nil {
					return
				}
				defer upstream.Close()
				_ = upstream.SetDeadline(time.Now().Add(10 * time.Second))
				done := make(chan struct{})
				go func() { _, _ = io.Copy(upstream, client); _ = upstream.Close(); close(done) }()
				_, _ = io.Copy(client, upstream)
				_ = client.Close()
				<-done
			}()
		}
	}()
	defer func() {
		listener.Close()
		connections.Range(func(k, v any) bool { k.(net.Conn).Close(); return true })
		workers.Wait()
	}()
	cfg := config.RedisStreamsDataConnectorConfig{Servers: []string{"rediss://" + listener.Addr().String()}, Password: options.Password, TLSCAFile: caPath, TLSServerName: "redis.test", TLSCertFile: clientCert, TLSKeyFile: clientKey, ConnectTimeoutMs: 250, OperationTimeoutMs: 500}
	key := "sa-tls-" + xid.New().String()
	defer admin.Del(ctx, key)
	endpoint := &config.RedisStreamsEndpointConfig{StreamKey: key, CreateStream: true}
	session, err := Open(ctx, &cfg, endpoint, false)
	require.NoError(t, err)
	id, err := session.Publish(ctx, map[string]interface{}{"value": "secure"})
	require.NoError(t, err)
	require.NotEmpty(t, id)
	require.NoError(t, session.Close())
	for _, test := range []struct {
		name   string
		change func(*config.RedisStreamsDataConnectorConfig)
	}{
		{"wrong-server-name", func(c *config.RedisStreamsDataConnectorConfig) { c.TLSServerName = "wrong.test" }},
		{"untrusted-ca", func(c *config.RedisStreamsDataConnectorConfig) { c.TLSCAFile = "" }},
		{"missing-client-identity", func(c *config.RedisStreamsDataConnectorConfig) { c.TLSCertFile = ""; c.TLSKeyFile = "" }},
		{"wrong-password", func(c *config.RedisStreamsDataConnectorConfig) { c.Password = "incorrect-test-password" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := cfg
			test.change(&bad)
			failed, e := Open(ctx, &bad, endpoint, false)
			require.Error(t, e)
			if failed != nil {
				failed.Close()
			}
			require.NotContains(t, e.Error(), bad.Password)
		})
	}
	require.EqualValues(t, 1, admin.XLen(ctx, key).Val())
}
