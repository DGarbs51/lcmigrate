package config

import (
	"os"
	"testing"
)

func TestDefaultPort(t *testing.T) {
	tests := []struct {
		engine string
		want   string
	}{
		{"mysql", "3306"},
		{"pgsql", "5432"},
		{"postgres", "5432"},
		{"postgresql", "5432"},
		{"unknown", "3306"},
		{"", "3306"},
	}

	for _, tt := range tests {
		got := DefaultPort(tt.engine)
		if got != tt.want {
			t.Errorf("DefaultPort(%q) = %q, want %q", tt.engine, got, tt.want)
		}
	}
}

func TestNormalizeEngine(t *testing.T) {
	tests := []struct {
		engine string
		want   string
	}{
		{"mysql", "mysql"},
		{"mariadb", "mysql"},
		{"pgsql", "pgsql"},
		{"postgres", "pgsql"},
		{"postgresql", "pgsql"},
		{"unknown", "unknown"},
		{"", ""},
	}

	for _, tt := range tests {
		got := NormalizeEngine(tt.engine)
		if got != tt.want {
			t.Errorf("NormalizeEngine(%q) = %q, want %q", tt.engine, got, tt.want)
		}
	}
}

func TestGetEnvWithFallback(t *testing.T) {
	// Clean up any existing env vars
	os.Unsetenv("TEST_PRIMARY")
	os.Unsetenv("TEST_FALLBACK")

	// Test fallback when primary is not set
	os.Setenv("TEST_FALLBACK", "fallback_value")
	defer os.Unsetenv("TEST_FALLBACK")

	got := getEnvWithFallback("TEST_PRIMARY", "TEST_FALLBACK")
	if got != "fallback_value" {
		t.Errorf("getEnvWithFallback() = %q, want %q", got, "fallback_value")
	}

	// Test primary when both are set
	os.Setenv("TEST_PRIMARY", "primary_value")
	defer os.Unsetenv("TEST_PRIMARY")

	got = getEnvWithFallback("TEST_PRIMARY", "TEST_FALLBACK")
	if got != "primary_value" {
		t.Errorf("getEnvWithFallback() = %q, want %q", got, "primary_value")
	}

	// Test empty when none are set
	os.Unsetenv("TEST_PRIMARY")
	os.Unsetenv("TEST_FALLBACK")

	got = getEnvWithFallback("TEST_PRIMARY", "TEST_FALLBACK")
	if got != "" {
		t.Errorf("getEnvWithFallback() = %q, want empty string", got)
	}
}

func TestLoadSourceDefaults(t *testing.T) {
	// Clear all relevant env vars
	envVars := []string{
		"SOURCE_DB_ENGINE", "SOURCE_DB_CONNECTION", "DB_ENGINE", "DB_CONNECTION",
		"SOURCE_DB_HOST", "DB_HOST",
		"SOURCE_DB_PORT", "DB_PORT",
		"SOURCE_DB_DATABASE", "SOURCE_DB_NAME", "DB_DATABASE", "DB_NAME",
		"SOURCE_DB_USER", "SOURCE_DB_USERNAME", "DB_USER", "DB_USERNAME",
		"SOURCE_DB_PASSWORD", "DB_PASSWORD",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}

	// Test with empty env
	cfg := LoadSourceDefaults()
	if cfg.Engine != "" || cfg.Host != "" || cfg.Port != "" || cfg.Database != "" || cfg.User != "" || cfg.Password != "" {
		t.Errorf("LoadSourceDefaults() with empty env should return empty config, got %+v", cfg)
	}

	// Test with SOURCE_DB_* vars (highest priority)
	os.Setenv("SOURCE_DB_ENGINE", "pgsql")
	os.Setenv("SOURCE_DB_HOST", "source-host")
	os.Setenv("SOURCE_DB_PORT", "5433")
	os.Setenv("SOURCE_DB_DATABASE", "source-db")
	os.Setenv("SOURCE_DB_USER", "source-user")
	os.Setenv("SOURCE_DB_PASSWORD", "source-pass")
	defer func() {
		for _, v := range envVars {
			os.Unsetenv(v)
		}
	}()

	cfg = LoadSourceDefaults()
	if cfg.Engine != "pgsql" {
		t.Errorf("Engine = %q, want %q", cfg.Engine, "pgsql")
	}
	if cfg.Host != "source-host" {
		t.Errorf("Host = %q, want %q", cfg.Host, "source-host")
	}
	if cfg.Port != "5433" {
		t.Errorf("Port = %q, want %q", cfg.Port, "5433")
	}
	if cfg.Database != "source-db" {
		t.Errorf("Database = %q, want %q", cfg.Database, "source-db")
	}
	if cfg.User != "source-user" {
		t.Errorf("User = %q, want %q", cfg.User, "source-user")
	}
	if cfg.Password != "source-pass" {
		t.Errorf("Password = %q, want %q", cfg.Password, "source-pass")
	}

	// Test fallback to DB_* vars
	for _, v := range envVars {
		os.Unsetenv(v)
	}
	os.Setenv("DB_ENGINE", "mysql")
	os.Setenv("DB_HOST", "fallback-host")
	os.Setenv("DB_PORT", "3307")
	os.Setenv("DB_DATABASE", "fallback-db")
	os.Setenv("DB_USER", "fallback-user")
	os.Setenv("DB_PASSWORD", "fallback-pass")

	cfg = LoadSourceDefaults()
	if cfg.Engine != "mysql" {
		t.Errorf("Engine fallback = %q, want %q", cfg.Engine, "mysql")
	}
	if cfg.Host != "fallback-host" {
		t.Errorf("Host fallback = %q, want %q", cfg.Host, "fallback-host")
	}
}

func TestLoadDestinationDefaults(t *testing.T) {
	// Clear all relevant env vars
	envVars := []string{
		"DESTINATION_DB_ENGINE", "DESTINATION_DB_CONNECTION",
		"DESTINATION_DB_HOST",
		"DESTINATION_DB_PORT",
		"DESTINATION_DB_DATABASE", "DESTINATION_DB_NAME",
		"DESTINATION_DB_USER", "DESTINATION_DB_USERNAME",
		"DESTINATION_DB_PASSWORD",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}

	// Test with empty env
	cfg := LoadDestinationDefaults()
	if cfg.Engine != "" || cfg.Host != "" || cfg.Port != "" || cfg.Database != "" || cfg.User != "" || cfg.Password != "" {
		t.Errorf("LoadDestinationDefaults() with empty env should return empty config, got %+v", cfg)
	}

	// Test with DESTINATION_DB_* vars
	os.Setenv("DESTINATION_DB_ENGINE", "pgsql")
	os.Setenv("DESTINATION_DB_HOST", "dest-host")
	os.Setenv("DESTINATION_DB_PORT", "5434")
	os.Setenv("DESTINATION_DB_DATABASE", "dest-db")
	os.Setenv("DESTINATION_DB_USER", "dest-user")
	os.Setenv("DESTINATION_DB_PASSWORD", "dest-pass")
	defer func() {
		for _, v := range envVars {
			os.Unsetenv(v)
		}
	}()

	cfg = LoadDestinationDefaults()
	if cfg.Engine != "pgsql" {
		t.Errorf("Engine = %q, want %q", cfg.Engine, "pgsql")
	}
	if cfg.Host != "dest-host" {
		t.Errorf("Host = %q, want %q", cfg.Host, "dest-host")
	}
	if cfg.Port != "5434" {
		t.Errorf("Port = %q, want %q", cfg.Port, "5434")
	}
	if cfg.Database != "dest-db" {
		t.Errorf("Database = %q, want %q", cfg.Database, "dest-db")
	}
	if cfg.User != "dest-user" {
		t.Errorf("User = %q, want %q", cfg.User, "dest-user")
	}
	if cfg.Password != "dest-pass" {
		t.Errorf("Password = %q, want %q", cfg.Password, "dest-pass")
	}
}

func TestHasEnvDefaults(t *testing.T) {
	// Clear all relevant env vars
	envVars := []string{
		"SOURCE_DB_HOST", "DB_HOST",
		"SOURCE_DB_USER", "SOURCE_DB_USERNAME", "DB_USER", "DB_USERNAME",
		"SOURCE_DB_DATABASE", "SOURCE_DB_NAME", "DB_DATABASE", "DB_NAME",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}

	// Test with no env vars set
	if HasEnvDefaults() {
		t.Errorf("HasEnvDefaults() = true, want false when no env vars set")
	}

	// Test with Host set
	os.Setenv("DB_HOST", "localhost")
	defer os.Unsetenv("DB_HOST")

	if !HasEnvDefaults() {
		t.Errorf("HasEnvDefaults() = false, want true when DB_HOST is set")
	}

	os.Unsetenv("DB_HOST")

	// Test with User set
	os.Setenv("DB_USER", "root")
	defer os.Unsetenv("DB_USER")

	if !HasEnvDefaults() {
		t.Errorf("HasEnvDefaults() = false, want true when DB_USER is set")
	}

	os.Unsetenv("DB_USER")

	// Test with Database set
	os.Setenv("DB_DATABASE", "mydb")
	defer os.Unsetenv("DB_DATABASE")

	if !HasEnvDefaults() {
		t.Errorf("HasEnvDefaults() = false, want true when DB_DATABASE is set")
	}
}

func TestLoadEnv(t *testing.T) {
	// LoadEnv should not panic or error when .env file doesn't exist
	// This is a smoke test
	LoadEnv()
}
