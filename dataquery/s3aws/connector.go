package s3aws

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
)

// Connector shares one official client and HTTP connection pool per connector.
// Credentials use the AWS default provider chain, never architecture properties.
type Connector struct {
	cfg               config.S3AWSDataConnectorConfig
	mu                sync.RWMutex
	client            *s3.Client
	transport         interface{ CloseIdleConnections() }
	suppliedClient    *s3.Client
	suppliedTransport interface{ CloseIdleConnections() }
}

var _ runtime.ManagedDataConnector = (*Connector)(nil)

// NewConnector uses the native client configured by a service infrastructure
// maker. The supplied transport is owned by this connector and must not be
// shared with independently managed clients. Stop closes its idle connections;
// AWS clients themselves have no Close method. A nil transport is allowed when
// there is no transport cleanup. Registration belongs to the generated service.
func NewConnector(cfg *config.S3AWSDataConnectorConfig, client *s3.Client, transport interface{ CloseIdleConnections() }) (*Connector, error) {
	if cfg == nil || cfg.ID <= 0 || client == nil {
		return nil, fmt.Errorf("S3 connector requires a positive id and a client")
	}
	return &Connector{cfg: *cfg, transport: transport, suppliedClient: client, suppliedTransport: transport}, nil
}

func (c *Connector) GetID() int      { return c.cfg.ID }
func (c *Connector) GetName() string { return c.cfg.Name }
func (c *Connector) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.client != nil {
		return nil
	}
	if c.suppliedClient != nil {
		c.client, c.transport = c.suppliedClient, c.suppliedTransport
		return nil
	}
	// Let the native loader apply AWS_CA_BUNDLE before taking ownership of
	// the resolved transport. A plain http.Client prevents that SDK resolution.
	builder := awshttp.NewBuildableClient().WithTransportOptions(func(transport *http.Transport) {
		transport.Proxy = http.ProxyFromEnvironment
		transport.DialContext = (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext
		transport.ForceAttemptHTTP2 = true
		transport.MaxIdleConns = 100
		transport.IdleConnTimeout = 90 * time.Second
		transport.TLSHandshakeTimeout = 10 * time.Second
		transport.ExpectContinueTimeout = time.Second
	})
	options := []func(*awsconfig.LoadOptions) error{awsconfig.WithHTTPClient(builder)}
	if c.cfg.Region != "" {
		options = append(options, awsconfig.WithRegion(c.cfg.Region))
	}
	native, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return fmt.Errorf("load S3 runtime configuration: %w", err)
	}
	resolved, ok := native.HTTPClient.(*awshttp.BuildableClient)
	if !ok {
		return fmt.Errorf("S3 configuration returned an unsupported HTTP client")
	}
	transport := resolved.GetTransport()
	native.HTTPClient = &http.Client{Transport: transport, Timeout: resolved.GetTimeout()}
	if err := ctx.Err(); err != nil {
		transport.CloseIdleConnections()
		return err
	}
	if native.Region == "" {
		transport.CloseIdleConnections()
		return fmt.Errorf("S3 region is required in runtime configuration or AWS_REGION")
	}
	c.client = s3.NewFromConfig(native, func(o *s3.Options) {
		o.UsePathStyle = c.cfg.ForcePathStyle
		o.Retryer = aws.NopRetryer{}
		if c.cfg.Address != "" {
			o.BaseEndpoint = aws.String(c.cfg.Address)
		}
	})
	c.transport = transport
	return nil
}
func (c *Connector) StartAdmission(context.Context) error { return nil }
func (c *Connector) StopAdmission(context.Context)        {}
func (c *Connector) Stop(context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.client = nil
	if c.transport != nil {
		c.transport.CloseIdleConnections()
		c.transport = nil
	}
}
func (c *Connector) nativeClient() (*s3.Client, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.client == nil {
		return nil, fmt.Errorf("S3 connector is not started")
	}
	return c.client, nil
}
func MakeConnector(env runtime.RuntimeEnvironment, cfg *config.S3AWSDataConnectorConfig) (*Connector, error) {
	if cfg == nil || cfg.ID <= 0 {
		return nil, fmt.Errorf("S3 connector requires a positive id")
	}
	if cfg.Address != "" {
		address, err := url.Parse(cfg.Address)
		if err != nil || address.Host == "" || (address.Scheme != "http" && address.Scheme != "https") || address.User != nil || address.RawQuery != "" || address.Fragment != "" {
			return nil, fmt.Errorf("S3 address must be an HTTP(S) endpoint without credentials, query or fragment")
		}
	}
	if existing := env.GetManagedDataConnector(cfg.ID); existing != nil {
		connector, ok := existing.(*Connector)
		if !ok {
			return nil, fmt.Errorf("connector %d already has another runtime type", cfg.ID)
		}
		return connector, nil
	}
	connector := &Connector{cfg: *cfg}
	env.AddManagedDataConnector(connector)
	return connector, nil
}

func (c *Connector) GetObject(ctx context.Context, input *s3.GetObjectInput, options ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	client, err := c.nativeClient()
	if err != nil {
		return nil, err
	}
	return client.GetObject(ctx, input, options...)
}

func (c *Connector) HeadObject(ctx context.Context, input *s3.HeadObjectInput, options ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	client, err := c.nativeClient()
	if err != nil {
		return nil, err
	}
	return client.HeadObject(ctx, input, options...)
}

func (c *Connector) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, options ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	client, err := c.nativeClient()
	if err != nil {
		return nil, err
	}
	return client.ListObjectsV2(ctx, input, options...)
}

func (c *Connector) PutObject(ctx context.Context, input *s3.PutObjectInput, options ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	client, err := c.nativeClient()
	if err != nil {
		return nil, err
	}
	return client.PutObject(ctx, input, options...)
}

func (c *Connector) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, options ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	client, err := c.nativeClient()
	if err != nil {
		return nil, err
	}
	return client.DeleteObject(ctx, input, options...)
}
