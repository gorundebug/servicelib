package kafka

import (
	"github.com/gorundebug/servicelib/internal/kafkasecuritytest"
	"testing"
)

func TestKafkaAdapterSecurityBroker(t *testing.T) {
	kafkasecuritytest.Run(t, "sink", makeKafkaConfig)
}
