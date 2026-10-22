package config

import (
	"errors"

	"github.com/gorundebug/servicelib/api"
)

// ValidateSecurity rejects ambiguous transport settings before any Kafka dial.
// An omitted SASL mechanism retains the documented PLAIN default; an unknown
// nonempty mechanism or protocol must never downgrade to plaintext transport.
func (c *KafkaDataConnectorConfig) ValidateSecurity() error {
	if c == nil {
		return errors.New("Kafka connector configuration is missing")
	}
	switch c.SecurityProtocol {
	case api.PLAINTEXT:
		if c.Username != "" || c.Password != "" {
			return errors.New("Kafka plaintext connector must not configure SASL credentials")
		}
		return nil
	case api.SASLPLAINTEXT, api.SASLSSL:
		if c.Username == "" || c.Password == "" {
			return errors.New("Kafka SASL username and password must both be configured")
		}
		switch c.SaslMechanism {
		case "", api.PLAIN, api.SCRAMSHA256, api.SCRAMSHA512:
			return nil
		default:
			return errors.New("unsupported Kafka SASL mechanism")
		}
	default:
		return errors.New("unsupported Kafka security protocol")
	}
}
