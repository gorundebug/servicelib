package kafka

import (
	"crypto/tls"
	"strings"
	"testing"

	"github.com/IBM/sarama"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
)

func TestKafkaTransportRejectsSecurityDowngrade(t *testing.T) {
	for _, cfg := range []*config.KafkaDataConnectorConfig{
		nil,
		{SecurityProtocol: "SASL_SLL", Username: "private-user", Password: "private-secret"},
		{SecurityProtocol: api.SASLSSL, SaslMechanism: "SCRAM-typo", Username: "private-user", Password: "private-secret"},
		{SecurityProtocol: api.PLAINTEXT, Username: "private-user", Password: "private-secret"},
	} {
		got, err := makeKafkaConfig(cfg)
		if err == nil || got != nil || strings.Contains(err.Error(), "private-") {
			t.Fatal("unsafe transport configuration produced", err)
		}
	}
}

func TestKafkaTransportKeepsTLSAndSASL(t *testing.T) {
	for _, mechanism := range []api.KafkaSaslMechanism{"", api.PLAIN, api.SCRAMSHA256, api.SCRAMSHA512} {
		got, err := makeKafkaConfig(&config.KafkaDataConnectorConfig{SecurityProtocol: api.SASLSSL, SaslMechanism: mechanism, Username: "user", Password: "secret"})
		if err != nil {
			t.Fatal(err)
		}
		if !got.Net.TLS.Enable || got.Net.TLS.Config == nil || got.Net.TLS.Config.MinVersion < tls.VersionTLS12 || got.Net.TLS.Config.InsecureSkipVerify || !got.Net.SASL.Enable || got.Net.SASL.User != "user" || got.Net.SASL.Password != "secret" {
			t.Fatal("TLS/SASL policy lost")
		}
		expected := string(mechanism)
		if expected == "" {
			expected = "PLAIN"
		}
		if string(got.Net.SASL.Mechanism) != expected {
			t.Fatal("SASL mechanism changed", got.Net.SASL.Mechanism, expected)
		}
		if mechanism == api.SCRAMSHA256 || mechanism == api.SCRAMSHA512 {
			if got.Net.SASL.SCRAMClientGeneratorFunc == nil {
				t.Fatal("SCRAM client missing")
			}
		}
	}
	plain, err := makeKafkaConfig(&config.KafkaDataConnectorConfig{SecurityProtocol: api.PLAINTEXT})
	if err != nil || plain.Net.SASL.Enable || plain.Net.TLS.Enable {
		t.Fatal("explicit plaintext changed", err)
	}
}

func TestKafkaNativeInputUsesCommittedIsolation(t *testing.T) {
	cfg, err := makeKafkaConfig(&config.KafkaDataConnectorConfig{SecurityProtocol: api.PLAINTEXT})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Consumer.IsolationLevel != sarama.ReadCommitted {
		t.Fatal("Kafka inputs must not deliver aborted records")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}
