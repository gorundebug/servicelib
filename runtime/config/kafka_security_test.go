package config

import (
	"strings"
	"testing"

	"github.com/gorundebug/servicelib/api"
)

func TestKafkaConnectorSecurityRejectsDowngrades(t *testing.T) {
	for _, cfg := range []*KafkaDataConnectorConfig{
		nil,
		{},
		{SecurityProtocol: "SASL_SLL", Username: "private-user", Password: "private-secret"},
		{SecurityProtocol: "SSL", Username: "private-user", Password: "private-secret"},
		{SecurityProtocol: api.PLAINTEXT, Username: "private-user"},
		{SecurityProtocol: api.PLAINTEXT, Password: "private-secret"},
		{SecurityProtocol: api.SASLSSL, Username: "private-user"},
		{SecurityProtocol: api.SASLSSL, Password: "private-secret"},
		{SecurityProtocol: api.SASLSSL, Username: "private-user", Password: "private-secret", SaslMechanism: "SCRAM-typo"},
	} {
		if err := cfg.ValidateSecurity(); err == nil || strings.Contains(err.Error(), "private-") {
			t.Fatal("invalid Kafka security accepted or credentials exposed", err)
		}
	}
}

func TestKafkaConnectorSecurityPreservesSupportedModes(t *testing.T) {
	plain := &KafkaDataConnectorConfig{SecurityProtocol: api.PLAINTEXT}
	if err := plain.ValidateSecurity(); err != nil {
		t.Fatal(err)
	}
	for _, protocol := range []api.KafkaSecurityProtocol{api.SASLPLAINTEXT, api.SASLSSL} {
		for _, mechanism := range []api.KafkaSaslMechanism{"", api.PLAIN, api.SCRAMSHA256, api.SCRAMSHA512} {
			cfg := &KafkaDataConnectorConfig{SecurityProtocol: protocol, SaslMechanism: mechanism, Username: "user", Password: "secret"}
			if err := cfg.ValidateSecurity(); err != nil {
				t.Fatal(protocol, mechanism, err)
			}
		}
	}
}
