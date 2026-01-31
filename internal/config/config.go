package config

import (
	"os"

	"github.com/joho/godotenv"
)

// DatabaseConfig represents a single database connection configuration
type DatabaseConfig struct {
	Engine   string
	Host     string
	Port     string
	Database string
	User     string
	Password string
}

// MigrationConfig holds both source and destination configurations
type MigrationConfig struct {
	Source      DatabaseConfig
	Destination DatabaseConfig
	DryRun      bool
}

// getEnvWithFallback checks multiple environment variable keys and returns the first non-empty value
func getEnvWithFallback(keys ...string) string {
	for _, key := range keys {
		if val := os.Getenv(key); val != "" {
			return val
		}
	}
	return ""
}

// LoadEnv loads the .env file if present
func LoadEnv() {
	_ = godotenv.Load() // Silently ignore if .env doesn't exist
}

// LoadSourceDefaults loads source database defaults from environment variables
// Checks SOURCE_DB_* first, then falls back to unprefixed DB_* variables
func LoadSourceDefaults() DatabaseConfig {
	return DatabaseConfig{
		Engine:   getEnvWithFallback("SOURCE_DB_ENGINE", "SOURCE_DB_CONNECTION", "DB_ENGINE", "DB_CONNECTION"),
		Host:     getEnvWithFallback("SOURCE_DB_HOST", "DB_HOST"),
		Port:     getEnvWithFallback("SOURCE_DB_PORT", "DB_PORT"),
		Database: getEnvWithFallback("SOURCE_DB_DATABASE", "SOURCE_DB_NAME", "DB_DATABASE", "DB_NAME"),
		User:     getEnvWithFallback("SOURCE_DB_USER", "SOURCE_DB_USERNAME", "DB_USER", "DB_USERNAME"),
		Password: getEnvWithFallback("SOURCE_DB_PASSWORD", "DB_PASSWORD"),
	}
}

// LoadDestinationDefaults loads destination database defaults from environment variables
// Only checks DESTINATION_DB_* prefixed variables
func LoadDestinationDefaults() DatabaseConfig {
	return DatabaseConfig{
		Engine:   getEnvWithFallback("DESTINATION_DB_ENGINE", "DESTINATION_DB_CONNECTION"),
		Host:     os.Getenv("DESTINATION_DB_HOST"),
		Port:     os.Getenv("DESTINATION_DB_PORT"),
		Database: getEnvWithFallback("DESTINATION_DB_DATABASE", "DESTINATION_DB_NAME"),
		User:     getEnvWithFallback("DESTINATION_DB_USER", "DESTINATION_DB_USERNAME"),
		Password: os.Getenv("DESTINATION_DB_PASSWORD"),
	}
}

// HasEnvDefaults returns true if any source environment defaults are set
func HasEnvDefaults() bool {
	defaults := LoadSourceDefaults()
	return defaults.Host != "" || defaults.User != "" || defaults.Database != ""
}

// DefaultPort returns the default port for a given database engine
func DefaultPort(engine string) string {
	switch engine {
	case "pgsql", "postgres", "postgresql":
		return "5432"
	default:
		return "3306"
	}
}

// NormalizeEngine normalizes engine names to consistent values
func NormalizeEngine(engine string) string {
	switch engine {
	case "postgres", "postgresql":
		return "pgsql"
	case "mysql", "mariadb":
		return "mysql"
	default:
		return engine
	}
}
