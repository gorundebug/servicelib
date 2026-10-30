package runtime

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ParseLocalScheduleInterval recognizes the portable local-only interval form.
// Canonical integer milliseconds avoid language-specific duration parsers. An
// unrecognized expression is left to the existing cron parser. Temporal must
// not use this local, non-durable scheduling contract.
func ParseLocalScheduleInterval(expression string) (time.Duration, bool, error) {
	if !strings.HasPrefix(expression, "@every") {
		return 0, false, nil
	}
	invalid := func() (time.Duration, bool, error) {
		return 0, true, fmt.Errorf("local interval requires @every <10..86400000>ms")
	}
	if !strings.HasPrefix(expression, "@every ") || !strings.HasSuffix(expression, "ms") {
		return invalid()
	}
	digits := strings.TrimSuffix(strings.TrimPrefix(expression, "@every "), "ms")
	if len(digits) == 0 || len(digits) > 8 || digits[0] == '0' {
		return invalid()
	}
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return invalid()
		}
	}
	milliseconds, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || milliseconds < 10 || milliseconds > 86400000 {
		return invalid()
	}
	return time.Duration(milliseconds) * time.Millisecond, true, nil
}
