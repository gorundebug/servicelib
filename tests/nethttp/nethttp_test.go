/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package nethttp

import (
	"bytes"
	"context"
	"fmt"
	"github.com/gorundebug/servicelib/datasource"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/serde"
	"github.com/gorundebug/servicelib/transformation"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"
)

type RequestData struct {
	Text string `json:"text,omitempty"`
}

type RequestDataSerde struct{}

func (s *RequestDataSerde) SerializeObj(value interface{}) ([]byte, error) {
	v, ok := value.(*RequestData)
	if !ok {
		return nil, fmt.Errorf("value is not *RequestData")
	}
	return s.Serialize(v)
}

func (s *RequestDataSerde) DeserializeObj(data []byte) (interface{}, error) {
	return s.Deserialize(data)
}

func (s *RequestDataSerde) Serialize(value *RequestData) ([]byte, error) {
	return []byte{}, nil
}

func (s *RequestDataSerde) Deserialize(data []byte) (*RequestData, error) {
	value := &RequestData{}
	return value, nil
}

type MockServiceConfig struct {
	config.ServiceAppConfig `mapstructure:",squash"`
}

type MockService struct {
	runtime.ServiceApp
	serviceConfig          *MockServiceConfig
	done                   chan struct{}
	appSink                runtime.TypedStreamConsumer[*RequestData]
	inputRequest           runtime.TypedInputStream[*RequestData]
	inputRequestDataSource runtime.Consumer[*RequestData]
	requestData            *RequestData
}

func (s *MockService) GetSerde(valueType reflect.Type) (serde.Serializer, error) {
	switch valueType {
	case serde.GetSerdeType[RequestData]():
		{
			var ser serde.Serde[*RequestData] = &RequestDataSerde{}
			return ser, nil
		}

	}
	return nil, nil
}

func (s *MockService) StreamsInit(ctx context.Context) {
	s.done = make(chan struct{})
	s.inputRequest = transformation.Input[*RequestData]("InputRequest", s)
	s.inputRequestDataSource = datasource.NetHTTPEndpointConsumer[*RequestData](s.inputRequest)
	s.appSink = transformation.AppSink[*RequestData]("AppSink", s.inputRequest, s.consume)
}

func (s *MockService) consume(value *RequestData) error {
	s.requestData = value
	s.done <- struct{}{}
	return nil
}

func (s *MockService) SetConfig(config config.Config) {
}

func (s *MockService) StartService(ctx context.Context) error {
	s.StreamsInit(ctx)
	return s.ServiceApp.Start(ctx)
}

func (s *MockService) StopService(ctx context.Context) {
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(s.GetServiceConfig().ShutdownTimeout)*time.Millisecond)
	defer cancel()
	wg := sync.WaitGroup{}
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.ServiceApp.Stop(timeoutCtx)
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

func (s *MockService) sendRequest() error {
	url := "http://localhost:8080/data"
	data := []byte(`{"text":"OK"}`)

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("error sending request: %s", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	_, err = io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status code is %d", resp.StatusCode)
	}
	return nil
}

func TestNetHTTPEndpointConsumer(t *testing.T) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	signal.Notify(stop, syscall.SIGTERM)

	mainCtx := context.Background()
	timeoutCtx, cancel := context.WithTimeout(mainCtx, time.Duration(1000)*time.Second)
	defer cancel()

	configSettings := config.ConfigSettings{}
	service := runtime.MakeService[*MockService, *MockServiceConfig]("IncomeService", &configSettings)
	if err := service.StartService(mainCtx); err != nil {
		assert.Equal(t, nil, err)
		return
	}
	go func() {
		defer func() { service.done <- struct{}{} }()
		assert.Equal(t, nil, service.sendRequest())
		assert.NotNilf(t, service.requestData, "request data is nil")
		if service.requestData != nil {
			assert.Equal(t, "OK", service.requestData.Text)
		}
	}()
	select {
	case <-service.done:
	case <-timeoutCtx.Done():
		t.Errorf("TestNetHTTPEndpointConsumer timeout: %s", timeoutCtx.Err())
	}
	service.StopService(mainCtx)
}
