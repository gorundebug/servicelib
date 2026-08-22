/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

// Package sarama exports the metrics maintained by Sarama's documented
// MetricRegistry through the framework metrics backend. The registry is read
// only when the backend collects an observable gauge, so this bridge adds no
// work to Kafka's publish and consume paths.
package sarama

import (
	"fmt"
	"strings"

	"github.com/gorundebug/servicelib/runtime/environment/metrics"
)

// Registry is the public subset of go-metrics.Registry used by the bridge.
// Sarama's Config.MetricRegistry implements it.
type Registry interface {
	Each(func(string, interface{}))
	Get(string) interface{}
}

type countMetric interface {
	Count() int64
}

type meterMetric interface {
	Count() int64
	Rate1() float64
}

// Role identifies which Sarama client owns a registry.
type Role string

const (
	RoleProducer Role = "producer"
	RoleConsumer Role = "consumer"
)

type observable struct {
	name     string
	help     string
	registry string
	value    func(interface{}) float64
	roles    map[Role]struct{}
}

var observables = []observable{
	{name: "requests_count", help: "Sarama broker requests since client start", registry: "request-rate", value: metricCount},
	{name: "request_rate", help: "Sarama one-minute broker request rate", registry: "request-rate", value: meterRate},
	{name: "responses_count", help: "Sarama broker responses since client start", registry: "response-rate", value: metricCount},
	{name: "response_rate", help: "Sarama one-minute broker response rate", registry: "response-rate", value: meterRate},
	{name: "bytes_sent_count", help: "Bytes sent by Sarama since client start", registry: "outgoing-byte-rate", value: metricCount},
	{name: "bytes_sent_rate", help: "Sarama one-minute outgoing byte rate", registry: "outgoing-byte-rate", value: meterRate},
	{name: "bytes_received_count", help: "Bytes received by Sarama since client start", registry: "incoming-byte-rate", value: metricCount},
	{name: "bytes_received_rate", help: "Sarama one-minute incoming byte rate", registry: "incoming-byte-rate", value: meterRate},
	{name: "requests_in_flight", help: "Sarama broker requests currently awaiting a response", registry: "requests-in-flight", value: metricCount},
	{name: "records_sent_count", help: "Records sent by the Sarama producer since client start", registry: "record-send-rate", value: metricCount, roles: roles(RoleProducer)},
	{name: "record_send_rate", help: "Sarama producer one-minute record send rate", registry: "record-send-rate", value: meterRate, roles: roles(RoleProducer)},
	{name: "fetches_count", help: "Fetch requests made by the Sarama consumer since client start", registry: "consumer-fetch-rate", value: metricCount, roles: roles(RoleConsumer)},
	{name: "fetch_rate", help: "Sarama consumer one-minute fetch request rate", registry: "consumer-fetch-rate", value: meterRate, roles: roles(RoleConsumer)},
}

func roles(values ...Role) map[Role]struct{} {
	result := make(map[Role]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func metricCount(value interface{}) float64 {
	if metric, ok := value.(countMetric); ok {
		return float64(metric.Count())
	}
	return 0
}

func meterRate(value interface{}) float64 {
	if metric, ok := value.(meterMetric); ok {
		return metric.Rate1()
	}
	return 0
}

func registryValue(registry Registry, name string, value func(interface{}) float64) func() float64 {
	return func() float64 { return value(registry.Get(name)) }
}

func registryPrefixCount(registry Registry, prefix string) func() float64 {
	return func() float64 {
		count := 0
		registry.Each(func(name string, _ interface{}) {
			if strings.HasPrefix(name, prefix) {
				count++
			}
		})
		return float64(count)
	}
}

// Register exposes one Sarama client registry. consumerGroup is required only
// for a consumer and is used to address Sarama's documented group metrics.
func Register(target metrics.Metrics, registry Registry, role Role, consumerGroup string) error {
	if target == nil || registry == nil {
		return nil
	}
	if role != RoleProducer && role != RoleConsumer {
		return fmt.Errorf("unsupported Sarama metrics role %q", role)
	}

	scope := target.Scope("sarama_kafka_client", metrics.Labels{"role": string(role)})
	for _, metric := range observables {
		if len(metric.roles) != 0 {
			if _, ok := metric.roles[role]; !ok {
				continue
			}
		}
		if err := scope.ObservableFloat64Gauge(
			metric.name, metric.help,
			registryValue(registry, metric.registry, metric.value),
		); err != nil {
			return fmt.Errorf("register Sarama metric %q: %w", metric.name, err)
		}
	}
	if err := scope.ObservableFloat64Gauge(
		"broker_metric_sets",
		"Broker-specific metric sets currently registered by Sarama",
		registryPrefixCount(registry, "request-rate-for-broker-"),
	); err != nil {
		return fmt.Errorf("register Sarama broker metric sets: %w", err)
	}

	if role == RoleConsumer && consumerGroup != "" {
		groupMetrics := []struct{ name, help, registry string }{
			{"consumer_group_joins_count", "Sarama consumer-group join attempts", "consumer-group-join-total-" + consumerGroup},
			{"consumer_group_join_failures_count", "Failed Sarama consumer-group joins", "consumer-group-join-failed-" + consumerGroup},
			{"consumer_group_syncs_count", "Sarama consumer-group sync attempts", "consumer-group-sync-total-" + consumerGroup},
			{"consumer_group_sync_failures_count", "Failed Sarama consumer-group syncs", "consumer-group-sync-failed-" + consumerGroup},
		}
		for _, metric := range groupMetrics {
			if err := scope.ObservableFloat64Gauge(
				metric.name, metric.help,
				registryValue(registry, metric.registry, metricCount),
			); err != nil {
				return fmt.Errorf("register Sarama metric %q: %w", metric.name, err)
			}
		}
	}
	return nil
}
