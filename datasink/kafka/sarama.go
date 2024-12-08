/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package kafka

import (
	"context"
	"github.com/gorundebug/servicelib/runtime"
)

type SaramaKafkaInputDataSource interface {
	runtime.DataSource
}

type SaramaKafkaDataSource struct {
	*runtime.InputDataSource
	done chan struct{}
}

type SaramaKafkaEndpoint struct {
	*runtime.DataSourceEndpoint
}

func (ds *SaramaKafkaDataSource) Start(ctx context.Context) error {
	return nil
}

func (ds *SaramaKafkaDataSource) Stop(ctx context.Context) {

}

func getSaramaKafkaDataSource(id int, env runtime.ServiceExecutionEnvironment) runtime.DataSource {
	dataSource := env.GetDataSource(id)
	if dataSource != nil {
		return dataSource
	}
	cfg := env.AppConfig().GetDataConnectorById(id)

	kafkaDataSource := &SaramaKafkaDataSource{
		InputDataSource: runtime.MakeInputDataSource(cfg, env),
		done:            make(chan struct{}),
	}
	var inputDataSource SaramaKafkaInputDataSource = kafkaDataSource
	env.AddDataSource(inputDataSource)
	return inputDataSource
}

func getSaramaKafkaDataSourceEndpoint(id int, env runtime.ServiceExecutionEnvironment) runtime.InputEndpoint {
	cfg := env.AppConfig().GetEndpointConfigById(id)
	dataSource := getSaramaKafkaDataSource(cfg.IdDataConnector, env)
	endpoint := dataSource.GetEndpoint(id)
	if endpoint != nil {
		return endpoint
	}
	kafkaEndpoint := &SaramaKafkaEndpoint{
		DataSourceEndpoint: runtime.MakeDataSourceEndpoint(dataSource, cfg.Id, env),
	}
	var inputEndpoint runtime.InputEndpoint = kafkaEndpoint
	dataSource.AddEndpoint(inputEndpoint)
	return kafkaEndpoint
}
