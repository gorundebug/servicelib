/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package mockservice

import (
	"context"
	"fmt"
	"github.com/gorundebug/servicelib/datasource"
	"github.com/gorundebug/servicelib/runtime"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/serde"
	"github.com/gorundebug/servicelib/transformation"
	"os"
	"path"
	"reflect"
	"sync"
	"time"
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

type MockServiceConfig struct {
	config.ServiceAppConfig `mapstructure:",squash"`
}

type MockService struct {
	runtime.ServiceApp
	serviceConfig          *MockServiceConfig //nolint:unused
	appSink                runtime.TypedStreamConsumer[*RequestData]
	inputRequest           runtime.TypedInputStream[*RequestData]
	inputRequestDataSource runtime.Consumer[*RequestData]
	RequestData            *RequestData
}

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

func (s *MockService) StreamsInit(ctx context.Context) {
	s.inputRequest = transformation.Input[*RequestData]("InputRequest", s)
	s.inputRequestDataSource = datasource.NetHTTPEndpointConsumer[*RequestData](s.inputRequest, nil)
	s.appSink = transformation.AppSink[*RequestData]("AppSink", s.inputRequest, s.consume)
}

func (s *MockService) consume(value *RequestData) error {
	s.RequestData = value
	return nil
}

func (s *MockService) SetConfig(config config.Config) {
}

func (s *MockService) StartService(ctx context.Context) error {
	s.StreamsInit(ctx)
	return s.ServiceApp.Start(ctx)
}

func (s *MockService) StopService(ctx context.Context) {
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(s.ServiceConfig().ShutdownTimeout)*time.Millisecond)
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

var service *MockService

func GetMockService() *MockService {
	return service
}

func Main(dir string, run func() int) int {
	_ = os.Chdir(path.Join(dir, "mockservice"))

	mainCtx := context.Background()

	configSettings := config.ConfigSettings{}
	var err error
	service, err = runtime.MakeService[*MockService, *MockServiceConfig]("IncomeService", nil, &configSettings)
	if err != nil {
		panic(err)
	}

	if err := service.StartService(mainCtx); err != nil {
		panic(err)
	}

	code := run()

	timeoutCtx, cancel := context.WithTimeout(mainCtx, time.Duration(10)*time.Second)
	defer cancel()
	service.StopService(timeoutCtx)

	service = nil

	return code
}
