package environment

import "testing"

func TestFlagEnabled(t *testing.T) {
	for _, value := range []string{"1", "true", "TRUE", " yes ", "On"} {
		t.Run("enabled_"+value, func(t *testing.T) {
			t.Setenv("SERVICELIB_TEST_BOOLEAN_FLAG", value)
			if !FlagEnabled("SERVICELIB_TEST_BOOLEAN_FLAG") {
				t.Fatalf("FlagEnabled() = false for %q", value)
			}
		})
	}
	for _, value := range []string{"", "0", "false", "no", "off", "anything"} {
		t.Run("disabled_"+value, func(t *testing.T) {
			t.Setenv("SERVICELIB_TEST_BOOLEAN_FLAG", value)
			if FlagEnabled("SERVICELIB_TEST_BOOLEAN_FLAG") {
				t.Fatalf("FlagEnabled() = true for %q", value)
			}
		})
	}
}
