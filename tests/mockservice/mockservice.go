/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package mockservice

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"net/http"

	"github.com/gorundebug/servicelib/datasink"
	"github.com/gorundebug/servicelib/datasource"
	httpds "github.com/gorundebug/servicelib/datasource/http"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/environment"
	"github.com/gorundebug/servicelib/runtime/environment/log"
	"github.com/gorundebug/servicelib/runtime/environment/metrics"
	"github.com/gorundebug/servicelib/runtime/environment/tracing"
	"github.com/gorundebug/servicelib/runtime/serde"
	"github.com/gorundebug/servicelib/runtime/testlog"
	"github.com/gorundebug/servicelib/runtime/testmetrics"
	"github.com/gorundebug/servicelib/runtime/testtracing"
	"github.com/gorundebug/servicelib/tests/mockservice/config"
	"github.com/gorundebug/servicelib/transformation"
)

type RequestData struct {
	Text string `json:"text,omitempty"`
}

type RequestDataSerde struct{}

func (s *RequestDataSerde) IsStub() bool {
	return false
}

func (s *RequestDataSerde) SerializeObj(value interface{}, b []byte) ([]byte, error) {
	v, ok := value.(*RequestData)
	if !ok {
		return nil, fmt.Errorf("value is not *RequestData")
	}
	return s.Serialize(v, b)
}

func (s *RequestDataSerde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *RequestDataSerde) Serialize(value *RequestData, b []byte) ([]byte, error) {
	return b, nil
}

func (s *RequestDataSerde) Deserialize(data []byte) (*RequestData, error) {
	value := &RequestData{}
	return value, nil
}

type MockService struct {
	runtime.ServiceApp
	sink                       runtime.TypedSinkStream[*RequestData, error]
	inputRequest               runtime.TypedInputStream[*RequestData, any, error]
	sink4                      runtime.TypedSinkStream[*RequestData, error]
	inputRequest3              runtime.TypedInputStream[*RequestData, any, error]
	sinkSync                   runtime.TypedSinkStream[*RequestData, error]
	inputRequestSync           runtime.TypedInputStream[*RequestData, any, error]
	inputRequestDataSource     runtime.Consumer[*RequestData]
	inputRequestDataSourceSync runtime.Consumer[*RequestData]
	streamSink                 runtime.Consumer[*RequestData]
	stream4Sink                runtime.Consumer[*RequestData]
	streamSinkSync             runtime.Consumer[*RequestData]
	endpointSink               *EndpointSink
	endpointSink4              *EndpointSink4
	endpointSinkSync           *EndpointSinkSync
	dataHandler                httpds.HTTPHandler

	RequestData atomic.Pointer[RequestData]
}

func (s *MockService) GetConfig() *config.Config {
	return s.ServiceApp.GetConfig().(*config.Config)
}

func (s *MockService) RegisterGRPCHandler(_ runtime.Endpoint, _ any) {}

func (s *MockService) GetSerde(valueType reflect.Type) (serde.Serializer, error) {
	switch valueType {
	case serde.GetSerdeType[RequestData](), serde.GetSerdeType[*RequestData]():
		{
			var ser serde.Serde[*RequestData] = &RequestDataSerde{}
			return ser, nil
		}
	}
	return nil, nil
}

type requestDataHandler struct{}

func (h *requestDataHandler) BeginRequest(ctx context.Context, _ httpds.StreamContext[*RequestData, any, error], _ httpds.HandlerData) (context.Context, struct{}, error) {
	return ctx, struct{}{}, nil
}

func (h *requestDataHandler) ConsumeMessage(ctx context.Context, sc httpds.StreamContext[*RequestData, any, error], _ struct{}, data httpds.HandlerData, resultCtx httpds.ResultContext[struct{}, *RequestData, any, *RequestData, any, error]) error {
	defer resultCtx.Done()
	body, err := io.ReadAll(data.Request.Body)
	if err != nil {
		return nil
	}
	var rd RequestData
	if err := json.Unmarshal(body, &rd); err != nil {
		return nil
	}
	sc.Collect(ctx, &rd)
	return nil
}

func (h *requestDataHandler) GetMessageID(_ context.Context, _ httpds.StreamContext[*RequestData, any, error], _ struct{}, _ any) string {
	return ""
}

func (h *requestDataHandler) EndRequest(_ context.Context, _ httpds.StreamContext[*RequestData, any, error], _ error, _ struct{}, _ httpds.HandlerData) {
}

func (s *MockService) buildRuntime(ctx context.Context) error {
	cfg := s.GetConfig()

	s.endpointSink = MakeEndpointSink(ctx, s, s)
	s.endpointSink4 = MakeEndpointSink4(ctx, s, s)
	s.endpointSinkSync = MakeEndpointSinkSync(ctx, s, s)

	var err error
	s.inputRequest, err = transformation.Input[*RequestData, any, error](&cfg.Streams.InputRequest, s)
	if err != nil {
		return err
	}
	s.inputRequest3, err = transformation.Input[*RequestData, any, error](&cfg.Streams.InputRequest3, s)
	if err != nil {
		return err
	}
	s.inputRequestSync, err = transformation.Input[*RequestData, any, error](&cfg.Streams.InputRequestSync, s)
	if err != nil {
		return err
	}

	s.inputRequestDataSource, s.dataHandler, err = datasource.NetHTTPEndpointConsumer[struct{}, *RequestData, any, *RequestData, any, error](s.inputRequest, &requestDataHandler{})
	if err != nil {
		return err
	}

	var dataSyncHandler httpds.HTTPHandler
	s.inputRequestDataSourceSync, dataSyncHandler, err = datasource.NetHTTPEndpointConsumer[struct{}, *RequestData, any, *RequestData, any, error](s.inputRequestSync, &requestDataHandler{})
	if err != nil {
		return err
	}
	s.RegisterHTTPHandler(cfg.Endpoints.DataSync.Path, http.HandlerFunc(dataSyncHandler))
	s.sink, err = transformation.Sink[*RequestData, error](&cfg.Streams.Sink, s.inputRequest)
	if err != nil {
		return err
	}
	s.sink4, err = transformation.Sink[*RequestData, error](&cfg.Streams.Sink4, s.inputRequest3)
	if err != nil {
		return err
	}
	s.sinkSync, err = transformation.Sink[*RequestData, error](&cfg.Streams.SinkSync, s.inputRequestSync)
	if err != nil {
		return err
	}
	s.streamSink, err = datasink.CustomEndpointConsumer[struct{}, *RequestData, error](s.sink, s.endpointSink)
	if err != nil {
		return err
	}
	s.stream4Sink, err = datasink.CustomEndpointConsumer[struct{}, *RequestData, error](s.sink4, s.endpointSink4)
	if err != nil {
		return err
	}
	s.streamSinkSync, err = datasink.CustomEndpointConsumer[struct{}, *RequestData, error](s.sinkSync, s.endpointSinkSync)
	if err != nil {
		return err
	}
	return nil
}

func (s *MockService) StartService(ctx context.Context) error {
	cfg := s.GetConfig()

	if err := s.buildRuntime(ctx); err != nil {
		return fmt.Errorf("failed to build runtime: %w", err)
	}

	s.RegisterHTTPHandler(cfg.Endpoints.Data.Path, http.HandlerFunc(s.dataHandler))

	return s.Start(ctx)
}

func (s *MockService) StopService(ctx context.Context) {
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(s.ServiceConfig().ShutdownTimeout)*time.Millisecond)
	defer cancel()
	wg := sync.WaitGroup{}
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Stop(timeoutCtx)
	}()
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-timeoutCtx.Done():
	}
}

// TestEnv holds all test-scoped dependencies created by Main.
// Pass it to individual test functions via a package-level var set inside the
// run callback so each test binary has its own isolated copy.
type TestEnv struct {
	Service *MockService
	Metrics *testmetrics.TestMetrics
	Tracing *testtracing.TestTracing
	Log     *testlog.TestLog
}

type testDeps struct {
	metrics *testmetrics.TestMetrics
	tracing *testtracing.TestTracing
	logs    *testlog.TestLog
}

func (d *testDeps) MetricsEngine(_ context.Context, _ environment.ServiceEnvironment) (metrics.MetricsEngine, error) {
	return d.metrics, nil
}

func (d *testDeps) TracingEngine(_ context.Context, _ environment.ServiceEnvironment) (tracing.TracingEngine, error) {
	return d.tracing, nil
}

func (d *testDeps) LogsEngine(_ context.Context, _ environment.ServiceEnvironment) (log.LogsEngine, error) {
	return d.logs, nil
}

func Main(dir string, run func(*TestEnv) int) int {
	_ = os.Chdir(path.Join(dir, "mockservice"))

	mainCtx := context.Background()

	baseConfigPath := "config/config.yaml"
	overrideConfigPath := "config/overrides.yaml"

	env := &TestEnv{
		Metrics: testmetrics.New(),
		Tracing: testtracing.New(),
		Log:     testlog.New(),
	}
	deps := &testDeps{metrics: env.Metrics, tracing: env.Tracing, logs: env.Log}

	var err error
	env.Service, err = runtime.MakeService[*MockService, *config.Config](
		mainCtx,
		"IncomeService",
		deps,
		&baseConfigPath,
		&overrideConfigPath,
		func() *MockService { return &MockService{} },
		func() *config.Config { return config.MakeConfig() },
	)
	if err != nil {
		panic(err)
	}

	if err := env.Service.StartService(mainCtx); err != nil {
		panic(err)
	}

	code := run(env)

	timeoutCtx, cancel := context.WithTimeout(mainCtx, time.Duration(10)*time.Second)
	defer cancel()
	env.Service.StopService(timeoutCtx)

	return code
}

type EndpointSink4 struct {
	mockService *MockService
}

func (ep *EndpointSink4) GetStreamID(_ context.Context, _ *RequestData) string {
	return runtime.NewStreamID()
}

func (ep *EndpointSink4) BeginRequest(ctx context.Context, _ runtime.Stream) (context.Context, struct{}) {
	return ctx, struct{}{}
}

func (ep *EndpointSink4) ConsumeMessage(_ context.Context, _ runtime.Stream, _ struct{}, value *RequestData, _ runtime.Collect[error]) error {
	ep.mockService.RequestData.Store(value)
	return nil
}

func (ep *EndpointSink4) EndRequest(_ context.Context, _ runtime.Stream, _ error, _ struct{}) {}

func MakeEndpointSink4(_ context.Context, _ environment.ServiceEnvironment, mockService *MockService) *EndpointSink4 {
	return &EndpointSink4{
		mockService: mockService,
	}
}

type EndpointSink struct {
	mockService *MockService
}

func (ep *EndpointSink) GetStreamID(_ context.Context, _ *RequestData) string {
	return runtime.NewStreamID()
}

func (ep *EndpointSink) BeginRequest(ctx context.Context, _ runtime.Stream) (context.Context, struct{}) {
	return ctx, struct{}{}
}

func (ep *EndpointSink) ConsumeMessage(_ context.Context, _ runtime.Stream, _ struct{}, value *RequestData, _ runtime.Collect[error]) error {
	ep.mockService.RequestData.Store(value)
	return nil
}

func (ep *EndpointSink) EndRequest(_ context.Context, _ runtime.Stream, _ error, _ struct{}) {}

func MakeEndpointSink(_ context.Context, _ environment.ServiceEnvironment, mockService *MockService) *EndpointSink {
	return &EndpointSink{
		mockService: mockService,
	}
}

type EndpointSinkSync struct {
	mockService *MockService
}

func (ep *EndpointSinkSync) GetStreamID(_ context.Context, _ *RequestData) string {
	return runtime.NewStreamID()
}

func (ep *EndpointSinkSync) BeginRequest(ctx context.Context, _ runtime.Stream) (context.Context, struct{}) {
	return ctx, struct{}{}
}

func (ep *EndpointSinkSync) ConsumeMessage(_ context.Context, _ runtime.Stream, _ struct{}, value *RequestData, _ runtime.Collect[error]) error {
	ep.mockService.RequestData.Store(value)
	return nil
}

func (ep *EndpointSinkSync) EndRequest(_ context.Context, _ runtime.Stream, _ error, _ struct{}) {}

func MakeEndpointSinkSync(_ context.Context, _ environment.ServiceEnvironment, mockService *MockService) *EndpointSinkSync {
	return &EndpointSinkSync{
		mockService: mockService,
	}
}
