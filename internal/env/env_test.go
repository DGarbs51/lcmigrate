package env

import (
	"os"
	"testing"
)

func TestOSProvider_Get(t *testing.T) {
	// Set a test environment variable
	testKey := "LCMIGRATE_TEST_VAR"
	testValue := "test_value_123"
	os.Setenv(testKey, testValue)
	defer os.Unsetenv(testKey)

	p := NewOSProvider()

	// Test getting existing variable
	if got := p.Get(testKey); got != testValue {
		t.Errorf("Get(%q) = %q, want %q", testKey, got, testValue)
	}

	// Test getting non-existent variable
	if got := p.Get("NONEXISTENT_VAR_12345"); got != "" {
		t.Errorf("Get(nonexistent) = %q, want empty string", got)
	}
}

func TestOSProvider_GetWithFallback(t *testing.T) {
	// Set test environment variables
	os.Setenv("LCMIGRATE_FIRST", "first_value")
	os.Setenv("LCMIGRATE_SECOND", "second_value")
	defer os.Unsetenv("LCMIGRATE_FIRST")
	defer os.Unsetenv("LCMIGRATE_SECOND")

	p := NewOSProvider()

	// Test first key exists
	if got := p.GetWithFallback("LCMIGRATE_FIRST", "LCMIGRATE_SECOND"); got != "first_value" {
		t.Errorf("GetWithFallback() = %q, want %q", got, "first_value")
	}

	// Test fallback to second key
	if got := p.GetWithFallback("NONEXISTENT", "LCMIGRATE_SECOND"); got != "second_value" {
		t.Errorf("GetWithFallback() = %q, want %q", got, "second_value")
	}

	// Test no keys exist
	if got := p.GetWithFallback("NONEXISTENT1", "NONEXISTENT2"); got != "" {
		t.Errorf("GetWithFallback() = %q, want empty string", got)
	}
}

func TestMapProvider_Get(t *testing.T) {
	p := NewMapProvider(map[string]string{
		"KEY1": "value1",
		"KEY2": "value2",
	})

	// Test getting existing key
	if got := p.Get("KEY1"); got != "value1" {
		t.Errorf("Get(%q) = %q, want %q", "KEY1", got, "value1")
	}

	// Test getting non-existent key
	if got := p.Get("NONEXISTENT"); got != "" {
		t.Errorf("Get(nonexistent) = %q, want empty string", got)
	}
}

func TestMapProvider_GetWithFallback(t *testing.T) {
	p := NewMapProvider(map[string]string{
		"KEY1": "value1",
		"KEY2": "value2",
	})

	// Test first key exists
	if got := p.GetWithFallback("KEY1", "KEY2"); got != "value1" {
		t.Errorf("GetWithFallback() = %q, want %q", got, "value1")
	}

	// Test fallback to second key
	if got := p.GetWithFallback("NONEXISTENT", "KEY2"); got != "value2" {
		t.Errorf("GetWithFallback() = %q, want %q", got, "value2")
	}

	// Test no keys exist
	if got := p.GetWithFallback("NONEXISTENT1", "NONEXISTENT2"); got != "" {
		t.Errorf("GetWithFallback() = %q, want empty string", got)
	}
}

func TestMapProvider_Set(t *testing.T) {
	p := NewMapProvider(nil)

	p.Set("NEW_KEY", "new_value")

	if got := p.Get("NEW_KEY"); got != "new_value" {
		t.Errorf("Get() after Set() = %q, want %q", got, "new_value")
	}
}

func TestNewMapProvider_NilMap(t *testing.T) {
	p := NewMapProvider(nil)

	// Should not panic
	if got := p.Get("ANY_KEY"); got != "" {
		t.Errorf("Get() on nil map = %q, want empty string", got)
	}

	// Should be able to set
	p.Set("KEY", "VALUE")
	if got := p.Get("KEY"); got != "VALUE" {
		t.Errorf("Get() after Set() = %q, want %q", got, "VALUE")
	}
}
