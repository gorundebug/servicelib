// Package kafkasecuritytest exercises adapter-created configurations against an
// explicitly supplied isolated broker. It is imported only by adapter tests.
package kafkasecuritytest

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
)

func Run(t *testing.T, adapter string, build func(*config.KafkaDataConnectorConfig) (*sarama.Config, error)) {
	t.Helper()
	broker := os.Getenv("SA_KAFKA_SECURITY_BROKER")
	if broker == "" {
		t.Skip("isolated TLS/SASL broker not configured")
	}
	read := func(name string) string {
		data, err := os.ReadFile(filepath.Join(os.Getenv("SA_KAFKA_SECURITY_SECRETS"), name))
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(data))
	}
	cfg, err := build(&config.KafkaDataConnectorConfig{
		SecurityProtocol: api.SASLSSL, SaslMechanism: api.PLAIN,
		Username: read("username"), Password: read("password"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Net.TLS.Enable || cfg.Net.TLS.Config == nil || cfg.Net.TLS.Config.InsecureSkipVerify || !cfg.Net.SASL.Enable {
		t.Fatal("adapter did not preserve verified TLS/SASL")
	}
	cfg.Version = sarama.V2_6_0_0
	cfg.Net.DialTimeout = 2 * time.Second
	cfg.Net.ReadTimeout = 2 * time.Second
	cfg.Net.WriteTimeout = 2 * time.Second
	cfg.Metadata.Retry.Max = 0
	cfg.Metadata.Timeout = 3 * time.Second
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Consumer.Return.Errors = true
	mode := os.Getenv("SA_KAFKA_SECURITY_FAILURE")
	if mode == "password" {
		cfg.Net.SASL.Password = "deliberately-invalid-fixture-password"
	}
	client, err := sarama.NewClient([]string{broker}, cfg)
	if mode != "" {
		if client != nil {
			_ = client.Close()
		}
		if err == nil {
			t.Fatal("unsafe credentials or trust unexpectedly connected")
		}
		needle := "x509:"
		if mode == "password" {
			needle = "SASL"
		}
		if !strings.Contains(err.Error(), needle) {
			t.Fatalf("expected %s rejection, got %T: %v", needle, err, err)
		}
		t.Log("verified rejection:", mode)
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	topic := fmt.Sprintf("sdk-%s-security-%d", adapter, time.Now().UnixNano())
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := admin.DeleteTopic(topic); err != nil {
			t.Error(err)
		}
	}()
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	key := []byte("security-probe")
	value := []byte{0, 0, 0, 0, 1, 2, 4, 6}
	partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(key), Value: sarama.ByteEncoder(value)})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	pc, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	select {
	case msg := <-pc.Messages():
		if msg == nil || msg.Offset != offset || !bytes.Equal(msg.Key, key) || !bytes.Equal(msg.Value, value) {
			t.Fatal("round trip changed message")
		}
	case err := <-pc.Errors():
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("secure consumption timed out")
	}
	t.Log("TLS/SASL publication and consumption preserved exact key/value")
}
