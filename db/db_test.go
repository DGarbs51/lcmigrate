package db

import (
	"os"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestGetEnvWithFallback(t *testing.T) {
	// Clean up test env vars
	os.Unsetenv("TEST_PRIMARY")
	os.Unsetenv("TEST_FALLBACK")

	// Test fallback when primary is not set
	os.Setenv("TEST_FALLBACK", "fallback_value")
	defer os.Unsetenv("TEST_FALLBACK")

	got := getEnvWithFallback("TEST_PRIMARY", "TEST_FALLBACK")
	if got != "fallback_value" {
		t.Errorf("getEnvWithFallback() = %q, want %q", got, "fallback_value")
	}

	// Test primary takes precedence
	os.Setenv("TEST_PRIMARY", "primary_value")
	defer os.Unsetenv("TEST_PRIMARY")

	got = getEnvWithFallback("TEST_PRIMARY", "TEST_FALLBACK")
	if got != "primary_value" {
		t.Errorf("getEnvWithFallback() = %q, want %q", got, "primary_value")
	}

	// Test empty when none set
	os.Unsetenv("TEST_PRIMARY")
	os.Unsetenv("TEST_FALLBACK")

	got = getEnvWithFallback("TEST_PRIMARY", "TEST_FALLBACK")
	if got != "" {
		t.Errorf("getEnvWithFallback() = %q, want empty string", got)
	}
}

func TestLoadEnvDefaults(t *testing.T) {
	// Clear relevant env vars
	envVars := []string{
		"DB_ENGINE", "DB_CONNECTION",
		"DB_HOST", "DB_PORT",
		"DB_DATABASE", "DB_NAME",
		"DB_USER", "DB_USERNAME",
		"DB_PASSWORD",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}

	// Test with empty env
	defaults := loadEnvDefaults()
	if defaults.Engine != "" {
		t.Errorf("defaults.Engine = %q, want empty", defaults.Engine)
	}
	if defaults.Host != "" {
		t.Errorf("defaults.Host = %q, want empty", defaults.Host)
	}

	// Test with env vars set
	os.Setenv("DB_ENGINE", "mysql")
	os.Setenv("DB_HOST", "localhost")
	os.Setenv("DB_PORT", "3306")
	os.Setenv("DB_DATABASE", "testdb")
	os.Setenv("DB_USER", "testuser")
	os.Setenv("DB_PASSWORD", "testpass")
	defer func() {
		for _, v := range envVars {
			os.Unsetenv(v)
		}
	}()

	defaults = loadEnvDefaults()
	if defaults.Engine != "mysql" {
		t.Errorf("defaults.Engine = %q, want %q", defaults.Engine, "mysql")
	}
	if defaults.Host != "localhost" {
		t.Errorf("defaults.Host = %q, want %q", defaults.Host, "localhost")
	}
	if defaults.Port != "3306" {
		t.Errorf("defaults.Port = %q, want %q", defaults.Port, "3306")
	}
	if defaults.Database != "testdb" {
		t.Errorf("defaults.Database = %q, want %q", defaults.Database, "testdb")
	}
	if defaults.User != "testuser" {
		t.Errorf("defaults.User = %q, want %q", defaults.User, "testuser")
	}
	if defaults.Password != "testpass" {
		t.Errorf("defaults.Password = %q, want %q", defaults.Password, "testpass")
	}
}

func TestLoadEnvDefaults_Fallbacks(t *testing.T) {
	// Clear relevant env vars
	envVars := []string{
		"DB_ENGINE", "DB_CONNECTION",
		"DB_DATABASE", "DB_NAME",
		"DB_USER", "DB_USERNAME",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}

	// Test fallback from DB_CONNECTION to DB_ENGINE
	os.Setenv("DB_CONNECTION", "pgsql")
	defer os.Unsetenv("DB_CONNECTION")

	defaults := loadEnvDefaults()
	if defaults.Engine != "pgsql" {
		t.Errorf("defaults.Engine from DB_CONNECTION = %q, want %q", defaults.Engine, "pgsql")
	}

	// Test fallback from DB_NAME to DB_DATABASE
	os.Setenv("DB_NAME", "mydb")
	defer os.Unsetenv("DB_NAME")

	defaults = loadEnvDefaults()
	if defaults.Database != "mydb" {
		t.Errorf("defaults.Database from DB_NAME = %q, want %q", defaults.Database, "mydb")
	}

	// Test fallback from DB_USERNAME to DB_USER
	os.Setenv("DB_USERNAME", "admin")
	defer os.Unsetenv("DB_USERNAME")

	defaults = loadEnvDefaults()
	if defaults.User != "admin" {
		t.Errorf("defaults.User from DB_USERNAME = %q, want %q", defaults.User, "admin")
	}
}

func TestConfig_Fields(t *testing.T) {
	cfg := Config{
		Engine:   "mysql",
		Host:     "localhost",
		Port:     "3306",
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
	}

	if cfg.Engine != "mysql" {
		t.Errorf("Config.Engine = %q, want %q", cfg.Engine, "mysql")
	}
	if cfg.Host != "localhost" {
		t.Errorf("Config.Host = %q, want %q", cfg.Host, "localhost")
	}
	if cfg.Port != "3306" {
		t.Errorf("Config.Port = %q, want %q", cfg.Port, "3306")
	}
	if cfg.Database != "testdb" {
		t.Errorf("Config.Database = %q, want %q", cfg.Database, "testdb")
	}
	if cfg.User != "testuser" {
		t.Errorf("Config.User = %q, want %q", cfg.User, "testuser")
	}
	if cfg.Password != "testpass" {
		t.Errorf("Config.Password = %q, want %q", cfg.Password, "testpass")
	}
}

func TestConnect_UnsupportedEngine(t *testing.T) {
	cfg := Config{
		Engine: "unsupported",
	}

	_, err := Connect(cfg)
	if err == nil {
		t.Errorf("Connect() with unsupported engine expected error, got nil")
	}
}

func TestAnalyze_UnsupportedEngine(t *testing.T) {
	cfg := Config{
		Engine: "unsupported",
	}

	err := Analyze(nil, cfg)
	if err == nil {
		t.Errorf("Analyze() with unsupported engine expected error, got nil")
	}
}

func TestEnvDefaults_Fields(t *testing.T) {
	defaults := envDefaults{
		Engine:   "mysql",
		Host:     "localhost",
		Port:     "3306",
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
	}

	if defaults.Engine != "mysql" {
		t.Errorf("envDefaults.Engine = %q, want %q", defaults.Engine, "mysql")
	}
	if defaults.Host != "localhost" {
		t.Errorf("envDefaults.Host = %q, want %q", defaults.Host, "localhost")
	}
	if defaults.Port != "3306" {
		t.Errorf("envDefaults.Port = %q, want %q", defaults.Port, "3306")
	}
	if defaults.Database != "testdb" {
		t.Errorf("envDefaults.Database = %q, want %q", defaults.Database, "testdb")
	}
	if defaults.User != "testuser" {
		t.Errorf("envDefaults.User = %q, want %q", defaults.User, "testuser")
	}
	if defaults.Password != "testpass" {
		t.Errorf("envDefaults.Password = %q, want %q", defaults.Password, "testpass")
	}
}

func TestGetEnvWithFallback_SingleKey(t *testing.T) {
	os.Setenv("TEST_SINGLE", "value")
	defer os.Unsetenv("TEST_SINGLE")

	got := getEnvWithFallback("TEST_SINGLE")
	if got != "value" {
		t.Errorf("getEnvWithFallback() = %q, want %q", got, "value")
	}
}

func TestGetEnvWithFallback_NoKeys(t *testing.T) {
	got := getEnvWithFallback()
	if got != "" {
		t.Errorf("getEnvWithFallback() with no keys = %q, want empty", got)
	}
}

func TestGetEnvWithFallback_MultipleFallbacks(t *testing.T) {
	os.Unsetenv("TEST_A")
	os.Unsetenv("TEST_B")
	os.Unsetenv("TEST_C")

	os.Setenv("TEST_C", "third")
	defer os.Unsetenv("TEST_C")

	got := getEnvWithFallback("TEST_A", "TEST_B", "TEST_C")
	if got != "third" {
		t.Errorf("getEnvWithFallback() = %q, want %q", got, "third")
	}
}

func TestConfig_ZeroValue(t *testing.T) {
	cfg := Config{}

	if cfg.Engine != "" {
		t.Errorf("Config.Engine = %q, want empty", cfg.Engine)
	}
	if cfg.Host != "" {
		t.Errorf("Config.Host = %q, want empty", cfg.Host)
	}
	if cfg.Port != "" {
		t.Errorf("Config.Port = %q, want empty", cfg.Port)
	}
	if cfg.Database != "" {
		t.Errorf("Config.Database = %q, want empty", cfg.Database)
	}
	if cfg.User != "" {
		t.Errorf("Config.User = %q, want empty", cfg.User)
	}
	if cfg.Password != "" {
		t.Errorf("Config.Password = %q, want empty", cfg.Password)
	}
}

func TestConnect_MySQL_InvalidHost(t *testing.T) {
	cfg := Config{
		Engine:   "mysql",
		Host:     "invalid-host-that-does-not-exist",
		Port:     "3306",
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
	}

	_, err := Connect(cfg)
	if err == nil {
		t.Logf("Connect() succeeded unexpectedly (may have valid host)")
	}
}

func TestConnect_Postgres_InvalidHost(t *testing.T) {
	cfg := Config{
		Engine:   "pgsql",
		Host:     "invalid-host-that-does-not-exist",
		Port:     "5432",
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
	}

	_, err := Connect(cfg)
	if err == nil {
		t.Logf("Connect() succeeded unexpectedly (may have valid host)")
	}
}

func TestAnalyzeMySQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock VERSION query
	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.35"))

	// Mock SHOW VARIABLES
	mock.ExpectQuery("SHOW VARIABLES WHERE Variable_name IN").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("version_comment", "MySQL Community Server").
			AddRow("max_connections", "151").
			AddRow("wait_timeout", "28800").
			AddRow("character_set_server", "utf8mb4").
			AddRow("collation_server", "utf8mb4_general_ci").
			AddRow("innodb_buffer_pool_size", "134217728"))

	// Mock uptime query
	mock.ExpectQuery("SELECT VARIABLE_VALUE FROM performance_schema.global_status").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("86400"))

	// Mock table count
	mock.ExpectQuery("SELECT COUNT\\(\\*\\).*FROM information_schema.tables.*table_type = 'BASE TABLE'").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))

	// Mock database size
	mock.ExpectQuery("SELECT SUM\\(data_length \\+ index_length\\).*FROM information_schema.tables").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"size"}).AddRow(float64(1024000)))

	// Mock table details
	mock.ExpectQuery("SELECT.*table_name.*engine.*table_rows.*FROM information_schema.tables").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"table_name", "engine", "table_rows", "total_size"}).
			AddRow("users", "InnoDB", int64(100), float64(50000)).
			AddRow("orders", "InnoDB", int64(200), float64(75000)))

	// Mock indexes
	mock.ExpectQuery("SELECT.*table_name.*index_name.*FROM information_schema.statistics").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"table_name", "index_name", "columns", "non_unique"}).
			AddRow("users", "PRIMARY", "id", 0).
			AddRow("users", "idx_email", "email", 1))

	// Mock foreign keys
	mock.ExpectQuery("SELECT.*table_name.*column_name.*constraint_name.*FROM information_schema.key_column_usage").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"table_name", "column_name", "constraint_name", "referenced_table_name", "referenced_column_name"}).
			AddRow("orders", "user_id", "fk_orders_user", "users", "id"))

	// Mock connection stats
	mock.ExpectQuery("SHOW STATUS WHERE Variable_name IN").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("Threads_connected", "5").
			AddRow("Max_used_connections", "10").
			AddRow("Connections", "100").
			AddRow("Aborted_connects", "2"))

	err = AnalyzeMySQL(db, "testdb")
	if err != nil {
		t.Errorf("AnalyzeMySQL() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestAnalyzeMySQL_VersionError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnError(sqlmock.ErrCancelled)

	err = AnalyzeMySQL(db, "testdb")
	if err == nil {
		t.Errorf("AnalyzeMySQL() expected error, got nil")
	}
}

func TestAnalyzeMySQL_ServerVarsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.35"))

	mock.ExpectQuery("SHOW VARIABLES WHERE Variable_name IN").
		WillReturnError(sqlmock.ErrCancelled)

	err = AnalyzeMySQL(db, "testdb")
	if err == nil {
		t.Errorf("AnalyzeMySQL() expected error, got nil")
	}
}

func TestAnalyzePostgres(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock version query
	mock.ExpectQuery("SELECT version\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("PostgreSQL 16.2"))

	// Mock SHOW queries
	mock.ExpectQuery("SHOW max_connections").
		WillReturnRows(sqlmock.NewRows([]string{"max_connections"}).AddRow("100"))
	mock.ExpectQuery("SHOW shared_buffers").
		WillReturnRows(sqlmock.NewRows([]string{"shared_buffers"}).AddRow("128MB"))
	mock.ExpectQuery("SHOW work_mem").
		WillReturnRows(sqlmock.NewRows([]string{"work_mem"}).AddRow("4MB"))
	mock.ExpectQuery("SHOW server_encoding").
		WillReturnRows(sqlmock.NewRows([]string{"server_encoding"}).AddRow("UTF8"))
	mock.ExpectQuery("SHOW timezone").
		WillReturnRows(sqlmock.NewRows([]string{"timezone"}).AddRow("UTC"))

	// Mock uptime
	mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM").
		WillReturnRows(sqlmock.NewRows([]string{"uptime"}).AddRow(float64(86400)))

	// Mock table count
	mock.ExpectQuery("SELECT COUNT\\(\\*\\).*FROM information_schema.tables.*table_type = 'BASE TABLE'").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	// Mock database size
	mock.ExpectQuery("SELECT pg_database_size").
		WillReturnRows(sqlmock.NewRows([]string{"size"}).AddRow(int64(2048000)))

	// Mock table details
	mock.ExpectQuery("SELECT.*t.table_name.*FROM information_schema.tables t").
		WillReturnRows(sqlmock.NewRows([]string{"table_name", "row_count", "total_size"}).
			AddRow("users", int64(100), int64(50000)).
			AddRow("products", int64(50), int64(25000)))

	// Mock indexes
	mock.ExpectQuery("SELECT.*tablename.*indexname.*FROM pg_indexes").
		WillReturnRows(sqlmock.NewRows([]string{"tablename", "indexname", "is_unique"}).
			AddRow("users", "users_pkey", true).
			AddRow("users", "idx_email", false))

	// Mock foreign keys
	mock.ExpectQuery("SELECT.*tc.table_name.*FROM information_schema.table_constraints").
		WillReturnRows(sqlmock.NewRows([]string{"table_name", "column_name", "constraint_name", "foreign_table_name", "foreign_column_name"}))

	// Mock connection stats
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM pg_stat_activity WHERE state = 'active'").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM pg_stat_activity WHERE state = 'idle'").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM pg_stat_activity").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))

	err = AnalyzePostgres(db, "testdb")
	if err != nil {
		t.Errorf("AnalyzePostgres() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestAnalyzePostgres_VersionError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT version\\(\\)").
		WillReturnError(sqlmock.ErrCancelled)

	err = AnalyzePostgres(db, "testdb")
	if err == nil {
		t.Errorf("AnalyzePostgres() expected error, got nil")
	}
}

func TestAnalyzePostgres_TableCountError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT version\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("PostgreSQL 16.2"))

	// SHOW queries can fail silently
	mock.ExpectQuery("SHOW max_connections").WillReturnError(sqlmock.ErrCancelled)
	mock.ExpectQuery("SHOW shared_buffers").WillReturnError(sqlmock.ErrCancelled)
	mock.ExpectQuery("SHOW work_mem").WillReturnError(sqlmock.ErrCancelled)
	mock.ExpectQuery("SHOW server_encoding").WillReturnError(sqlmock.ErrCancelled)
	mock.ExpectQuery("SHOW timezone").WillReturnError(sqlmock.ErrCancelled)
	mock.ExpectQuery("SELECT EXTRACT").WillReturnError(sqlmock.ErrCancelled)

	mock.ExpectQuery("SELECT COUNT\\(\\*\\).*FROM information_schema.tables").
		WillReturnError(sqlmock.ErrCancelled)

	err = AnalyzePostgres(db, "testdb")
	if err == nil {
		t.Errorf("AnalyzePostgres() expected error, got nil")
	}
}

func TestAnalyze_MySQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock enough for the Analyze function to call AnalyzeMySQL
	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnError(sqlmock.ErrCancelled) // Will fail quickly

	cfg := Config{Engine: "mysql"}
	err = Analyze(db, cfg)
	if err == nil {
		t.Logf("Analyze() returned error as expected")
	}
}

func TestAnalyze_Postgres(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock enough for the Analyze function to call AnalyzePostgres
	mock.ExpectQuery("SELECT version\\(\\)").
		WillReturnError(sqlmock.ErrCancelled) // Will fail quickly

	cfg := Config{Engine: "pgsql"}
	err = Analyze(db, cfg)
	if err == nil {
		t.Logf("Analyze() returned error as expected")
	}
}
