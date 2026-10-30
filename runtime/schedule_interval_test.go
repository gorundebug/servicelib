package runtime

import (
	"strconv"
	"testing"
	"time"
)

func TestLocalScheduleIntervalContract(t *testing.T) {
	for _, milliseconds := range []time.Duration{10, 100, 1000, 86400000} {
		expression := "@every " + strconv.FormatInt(int64(milliseconds), 10) + "ms"
		actual, recognized, err := ParseLocalScheduleInterval(expression)
		if err != nil || !recognized || actual != milliseconds*time.Millisecond {
			t.Fatal(expression, actual, recognized, err)
		}
	}
	for _, expression := range []string{"@every", "@every 0ms", "@every 9ms", "@every 86400001ms", "@every 010ms", "@every +10ms", "@every -10ms", "@every 0.1s", "@every 100ms ", "@every  100ms", "@every 100000000000000000000ms", "@every \uff1100ms", "@everywhere 100ms"} {
		if _, recognized, err := ParseLocalScheduleInterval(expression); !recognized || err == nil {
			t.Fatal("invalid interval accepted", expression)
		}
	}
	if _, recognized, err := ParseLocalScheduleInterval("*/5 * * * *"); recognized || err != nil {
		t.Fatal("calendar expression intercepted", recognized, err)
	}
}
