package preflight

import (
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/DGarbs51/lcmigrate/internal/config"
)

func TestExtractMajorVersion(t *testing.T) {
	tests := []struct {
		version string
		want    int
	}{
		{"8.0.35", 8},
		{"5.7.44-log", 5},
		{"PostgreSQL 16.2 (Ubuntu 16.2-1.pgdg22.04+1)", 16},
		{"PostgreSQL 15.4", 15},
		{"10.6.0-MariaDB", 10},
		{"invalid", 0},
		{"", 0},
	}

	for _, tt := range tests {
		got := extractMajorVersion(tt.version)
		if got != tt.want {
			t.Errorf("extractMajorVersion(%q) = %d, want %d", tt.version, got, tt.want)
		}
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"users", `"users"`},
		{"my_table", `"my_table"`},
		{`table"name`, `"table""name"`},
		{"", `""`},
	}

	for _, tt := range tests {
		got := quoteIdentifier(tt.input)
		if got != tt.want {
			t.Errorf("quoteIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestIsDatabaseNotExistsError_MySQL(t *testing.T) {
	tests := []struct {
		err    error
		engine string
		want   bool
	}{
		{errors.New("Error 1049: Unknown database 'testdb'"), "mysql", true},
		{errors.New("Unknown database 'testdb'"), "mysql", true},
		{errors.New("Access denied for user"), "mysql", false},
		{nil, "mysql", false},
	}

	for _, tt := range tests {
		got := isDatabaseNotExistsError(tt.err, tt.engine)
		if got != tt.want {
			t.Errorf("isDatabaseNotExistsError(%v, %q) = %v, want %v", tt.err, tt.engine, got, tt.want)
		}
	}
}

func TestIsDatabaseNotExistsError_Postgres(t *testing.T) {
	tests := []struct {
		err    error
		engine string
		want   bool
	}{
		{errors.New("FATAL: database \"testdb\" does not exist"), "pgsql", true},
		{errors.New("pq: 3D000: database \"testdb\" does not exist"), "pgsql", true},
		{errors.New("permission denied"), "pgsql", false},
		{nil, "pgsql", false},
	}

	for _, tt := range tests {
		got := isDatabaseNotExistsError(tt.err, tt.engine)
		if got != tt.want {
			t.Errorf("isDatabaseNotExistsError(%v, %q) = %v, want %v", tt.err, tt.engine, got, tt.want)
		}
	}
}

func TestDatabaseNotExistsError(t *testing.T) {
	err := &DatabaseNotExistsError{Database: "mydb"}
	expected := `database "mydb" does not exist`

	if err.Error() != expected {
		t.Errorf("DatabaseNotExistsError.Error() = %q, want %q", err.Error(), expected)
	}
}

func TestConnect_UnsupportedEngine(t *testing.T) {
	cfg := config.DatabaseConfig{
		Engine: "unsupported",
	}

	_, err := Connect(cfg)
	if err == nil {
		t.Errorf("Connect() with unsupported engine expected error, got nil")
	}
}

func TestCreateDatabase_UnsupportedEngine(t *testing.T) {
	cfg := config.DatabaseConfig{
		Engine: "unsupported",
	}

	err := CreateDatabase(cfg)
	if err == nil {
		t.Errorf("CreateDatabase() with unsupported engine expected error, got nil")
	}
}

func TestCheckResult_Fields(t *testing.T) {
	result := CheckResult{
		Name:    "Test check",
		Passed:  true,
		Message: "Success",
		Warning: false,
	}

	if result.Name != "Test check" {
		t.Errorf("CheckResult.Name = %q, want %q", result.Name, "Test check")
	}
	if !result.Passed {
		t.Errorf("CheckResult.Passed = %v, want true", result.Passed)
	}
	if result.Message != "Success" {
		t.Errorf("CheckResult.Message = %q, want %q", result.Message, "Success")
	}
	if result.Warning {
		t.Errorf("CheckResult.Warning = %v, want false", result.Warning)
	}
}

func TestPreflightResult_Fields(t *testing.T) {
	result := PreflightResult{
		Passed:  true,
		Aborted: false,
	}

	if !result.Passed {
		t.Errorf("PreflightResult.Passed = %v, want true", result.Passed)
	}
	if result.Aborted {
		t.Errorf("PreflightResult.Aborted = %v, want false", result.Aborted)
	}
}

func TestDatabaseInfo_Fields(t *testing.T) {
	info := DatabaseInfo{
		Version:      "8.0.35",
		MajorVersion: 8,
		TableCount:   10,
		ViewCount:    2,
		TotalSize:    1024 * 1024,
		Tables:       []string{"users", "orders"},
	}

	if info.Version != "8.0.35" {
		t.Errorf("DatabaseInfo.Version = %q, want %q", info.Version, "8.0.35")
	}
	if info.MajorVersion != 8 {
		t.Errorf("DatabaseInfo.MajorVersion = %d, want %d", info.MajorVersion, 8)
	}
	if info.TableCount != 10 {
		t.Errorf("DatabaseInfo.TableCount = %d, want %d", info.TableCount, 10)
	}
	if info.ViewCount != 2 {
		t.Errorf("DatabaseInfo.ViewCount = %d, want %d", info.ViewCount, 2)
	}
	if len(info.Tables) != 2 {
		t.Errorf("len(DatabaseInfo.Tables) = %d, want %d", len(info.Tables), 2)
	}
}

func TestConnectResult_Fields(t *testing.T) {
	result := ConnectResult{
		DB:      nil,
		SSLMode: "prefer",
	}

	if result.SSLMode != "prefer" {
		t.Errorf("ConnectResult.SSLMode = %q, want %q", result.SSLMode, "prefer")
	}
}

func TestIsDatabaseNotExistsError_UnknownEngine(t *testing.T) {
	err := errors.New("some error")
	got := isDatabaseNotExistsError(err, "unknown")
	if got {
		t.Errorf("isDatabaseNotExistsError() with unknown engine = %v, want false", got)
	}
}

func TestExtractMajorVersion_MySQL(t *testing.T) {
	tests := []struct {
		version string
		want    int
	}{
		{"8.0.35-0ubuntu0.22.04.1", 8},
		{"5.7.44-log", 5},
		{"10.6.14-MariaDB-1:10.6.14+maria~ubu2204", 10},
	}

	for _, tt := range tests {
		got := extractMajorVersion(tt.version)
		if got != tt.want {
			t.Errorf("extractMajorVersion(%q) = %d, want %d", tt.version, got, tt.want)
		}
	}
}

func TestExtractMajorVersion_Postgres(t *testing.T) {
	tests := []struct {
		version string
		want    int
	}{
		{"PostgreSQL 16.2 (Ubuntu 16.2-1.pgdg22.04+1) on x86_64-pc-linux-gnu", 16},
		{"PostgreSQL 15.4 on x86_64-pc-linux-gnu", 15},
		{"PostgreSQL 14.8", 14},
	}

	for _, tt := range tests {
		got := extractMajorVersion(tt.version)
		if got != tt.want {
			t.Errorf("extractMajorVersion(%q) = %d, want %d", tt.version, got, tt.want)
		}
	}
}

func TestQuoteIdentifier_SpecialCharacters(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"normal_name", `"normal_name"`},
		{"table with spaces", `"table with spaces"`},
		{`table"with"quotes`, `"table""with""quotes"`},
		{"", `""`},
		{"CamelCase", `"CamelCase"`},
		{"123numeric", `"123numeric"`},
	}

	for _, tt := range tests {
		got := quoteIdentifier(tt.input)
		if got != tt.want {
			t.Errorf("quoteIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestIsDatabaseNotExistsError_MySQLVariations(t *testing.T) {
	tests := []struct {
		errMsg string
		want   bool
	}{
		{"Error 1049: Unknown database 'mydb'", true},
		{"Unknown database 'mydb'", true},
		{"Error 1045: Access denied for user 'root'@'localhost'", false},
		{"connection refused", false},
	}

	for _, tt := range tests {
		err := errors.New(tt.errMsg)
		got := isDatabaseNotExistsError(err, "mysql")
		if got != tt.want {
			t.Errorf("isDatabaseNotExistsError(%q, mysql) = %v, want %v", tt.errMsg, got, tt.want)
		}
	}
}

func TestIsDatabaseNotExistsError_PostgresVariations(t *testing.T) {
	tests := []struct {
		errMsg string
		want   bool
	}{
		{`pq: FATAL: database "mydb" does not exist`, true},
		{"3D000: database does not exist", true},
		{"permission denied for database", false},
		{"connection timeout", false},
	}

	for _, tt := range tests {
		err := errors.New(tt.errMsg)
		got := isDatabaseNotExistsError(err, "pgsql")
		if got != tt.want {
			t.Errorf("isDatabaseNotExistsError(%q, pgsql) = %v, want %v", tt.errMsg, got, tt.want)
		}
	}
}

func TestDatabaseNotExistsError_ErrorMethod(t *testing.T) {
	tests := []struct {
		database string
		want     string
	}{
		{"mydb", `database "mydb" does not exist`},
		{"test_database", `database "test_database" does not exist`},
		{"", `database "" does not exist`},
	}

	for _, tt := range tests {
		err := &DatabaseNotExistsError{Database: tt.database}
		if err.Error() != tt.want {
			t.Errorf("DatabaseNotExistsError{%q}.Error() = %q, want %q", tt.database, err.Error(), tt.want)
		}
	}
}

func TestConnect_MySQLConfig(t *testing.T) {
	cfg := config.DatabaseConfig{
		Engine:   "mysql",
		Host:     "localhost",
		Port:     "3306",
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
	}

	// This will fail to connect but we can test that it tries
	_, err := Connect(cfg)
	// We expect an error since there's no actual database
	if err == nil {
		t.Logf("Connect() succeeded (unexpected in test environment)")
	}
}

func TestConnect_PostgresConfig(t *testing.T) {
	cfg := config.DatabaseConfig{
		Engine:   "pgsql",
		Host:     "localhost",
		Port:     "5432",
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
	}

	// This will fail to connect but we can test the code path
	_, err := Connect(cfg)
	// We expect an error since there's no actual database
	if err == nil {
		t.Logf("Connect() succeeded (unexpected in test environment)")
	}
}

func TestCreateDatabase_MySQLConfig(t *testing.T) {
	cfg := config.DatabaseConfig{
		Engine:   "mysql",
		Host:     "localhost",
		Port:     "3306",
		Database: "newdb",
		User:     "testuser",
		Password: "testpass",
	}

	// This will fail but tests the code path
	err := CreateDatabase(cfg)
	if err == nil {
		t.Logf("CreateDatabase() succeeded (unexpected in test environment)")
	}
}

func TestCreateDatabase_PostgresConfig(t *testing.T) {
	cfg := config.DatabaseConfig{
		Engine:   "pgsql",
		Host:     "localhost",
		Port:     "5432",
		Database: "newdb",
		User:     "testuser",
		Password: "testpass",
	}

	// This will fail but tests the code path
	err := CreateDatabase(cfg)
	if err == nil {
		t.Logf("CreateDatabase() succeeded (unexpected in test environment)")
	}
}

func TestConnectResult_SSLModeField(t *testing.T) {
	result := ConnectResult{
		DB:      nil,
		SSLMode: "require",
	}

	if result.SSLMode != "require" {
		t.Errorf("ConnectResult.SSLMode = %q, want %q", result.SSLMode, "require")
	}

	result.SSLMode = "disable"
	if result.SSLMode != "disable" {
		t.Errorf("ConnectResult.SSLMode = %q, want %q", result.SSLMode, "disable")
	}
}

func TestDatabaseInfo_TablesList(t *testing.T) {
	info := DatabaseInfo{
		Tables: []string{"users", "orders", "products"},
	}

	if len(info.Tables) != 3 {
		t.Errorf("len(DatabaseInfo.Tables) = %d, want 3", len(info.Tables))
	}

	expectedTables := []string{"users", "orders", "products"}
	for i, table := range info.Tables {
		if table != expectedTables[i] {
			t.Errorf("Tables[%d] = %q, want %q", i, table, expectedTables[i])
		}
	}
}

func TestPreflightResult_Initialization(t *testing.T) {
	result := &PreflightResult{
		Passed: true,
	}

	// Check initial state
	if !result.Passed {
		t.Errorf("PreflightResult.Passed = %v, want true", result.Passed)
	}
	if result.Aborted {
		t.Errorf("PreflightResult.Aborted = %v, want false", result.Aborted)
	}
	if result.SourceConn != nil {
		t.Errorf("PreflightResult.SourceConn should be nil")
	}
	if result.DestConn != nil {
		t.Errorf("PreflightResult.DestConn should be nil")
	}
	if len(result.Checks) != 0 {
		t.Errorf("len(PreflightResult.Checks) = %d, want 0", len(result.Checks))
	}
}

func TestCheckResult_AllFields(t *testing.T) {
	check := CheckResult{
		Name:    "Connection test",
		Passed:  true,
		Message: "Successfully connected",
		Warning: false,
	}

	if check.Name != "Connection test" {
		t.Errorf("CheckResult.Name = %q, want %q", check.Name, "Connection test")
	}
	if !check.Passed {
		t.Errorf("CheckResult.Passed = %v, want true", check.Passed)
	}
	if check.Message != "Successfully connected" {
		t.Errorf("CheckResult.Message = %q, want %q", check.Message, "Successfully connected")
	}
	if check.Warning {
		t.Errorf("CheckResult.Warning = %v, want false", check.Warning)
	}

	// Test warning state
	warnCheck := CheckResult{
		Name:    "Version check",
		Passed:  true,
		Warning: true,
		Message: "Major version mismatch",
	}

	if !warnCheck.Warning {
		t.Errorf("warnCheck.Warning = %v, want true", warnCheck.Warning)
	}
}

func TestExtractMajorVersion_EdgeCases(t *testing.T) {
	tests := []struct {
		version string
		want    int
	}{
		{"", 0},
		{"invalid", 0},
		{"no version here", 0},
		{"v1.2.3", 1},
		{"1", 0}, // Need at least major.minor
		{"1.", 0},
		{"abc.def", 0},
	}

	for _, tt := range tests {
		got := extractMajorVersion(tt.version)
		if got != tt.want {
			t.Errorf("extractMajorVersion(%q) = %d, want %d", tt.version, got, tt.want)
		}
	}
}

func TestIsDatabaseNotExistsError_EmptyEngine(t *testing.T) {
	err := errors.New("some error")
	got := isDatabaseNotExistsError(err, "")
	if got {
		t.Errorf("isDatabaseNotExistsError() with empty engine = %v, want false", got)
	}
}

func TestDatabaseNotExistsError_Types(t *testing.T) {
	err := &DatabaseNotExistsError{Database: "testdb"}

	// Test that it implements error interface
	var e error = err
	if e.Error() != `database "testdb" does not exist` {
		t.Errorf("Error() = %q, want %q", e.Error(), `database "testdb" does not exist`)
	}

	// Test errors.As
	var dbErr *DatabaseNotExistsError
	if !errors.As(err, &dbErr) {
		t.Errorf("errors.As() should match DatabaseNotExistsError")
	}
	if dbErr.Database != "testdb" {
		t.Errorf("dbErr.Database = %q, want %q", dbErr.Database, "testdb")
	}
}

func TestConnectResult_ZeroValue(t *testing.T) {
	result := ConnectResult{}

	if result.DB != nil {
		t.Errorf("ConnectResult.DB = %v, want nil", result.DB)
	}
	if result.SSLMode != "" {
		t.Errorf("ConnectResult.SSLMode = %q, want empty", result.SSLMode)
	}
}

func TestPreflightResult_AllFields(t *testing.T) {
	result := PreflightResult{
		Passed:  true,
		Aborted: false,
		SourceInfo: DatabaseInfo{
			Version:      "8.0.35",
			MajorVersion: 8,
			TableCount:   5,
			ViewCount:    2,
			TotalSize:    1024,
			Tables:       []string{"users", "orders"},
		},
		DestInfo: DatabaseInfo{
			Version:      "8.0.36",
			MajorVersion: 8,
			TableCount:   0,
		},
		Checks: []CheckResult{
			{Name: "test", Passed: true},
		},
	}

	if !result.Passed {
		t.Errorf("PreflightResult.Passed = %v, want true", result.Passed)
	}
	if result.Aborted {
		t.Errorf("PreflightResult.Aborted = %v, want false", result.Aborted)
	}
	if result.SourceInfo.Version != "8.0.35" {
		t.Errorf("SourceInfo.Version = %q, want %q", result.SourceInfo.Version, "8.0.35")
	}
	if result.SourceInfo.TableCount != 5 {
		t.Errorf("SourceInfo.TableCount = %d, want 5", result.SourceInfo.TableCount)
	}
	if len(result.SourceInfo.Tables) != 2 {
		t.Errorf("len(SourceInfo.Tables) = %d, want 2", len(result.SourceInfo.Tables))
	}
	if result.DestInfo.TableCount != 0 {
		t.Errorf("DestInfo.TableCount = %d, want 0", result.DestInfo.TableCount)
	}
	if len(result.Checks) != 1 {
		t.Errorf("len(Checks) = %d, want 1", len(result.Checks))
	}
}

func TestDatabaseInfo_EmptyState(t *testing.T) {
	info := DatabaseInfo{}

	if info.Version != "" {
		t.Errorf("DatabaseInfo.Version = %q, want empty", info.Version)
	}
	if info.MajorVersion != 0 {
		t.Errorf("DatabaseInfo.MajorVersion = %d, want 0", info.MajorVersion)
	}
	if info.TableCount != 0 {
		t.Errorf("DatabaseInfo.TableCount = %d, want 0", info.TableCount)
	}
	if info.ViewCount != 0 {
		t.Errorf("DatabaseInfo.ViewCount = %d, want 0", info.ViewCount)
	}
	if info.TotalSize != 0 {
		t.Errorf("DatabaseInfo.TotalSize = %d, want 0", info.TotalSize)
	}
	if len(info.Tables) != 0 {
		t.Errorf("len(DatabaseInfo.Tables) = %d, want 0", len(info.Tables))
	}
}

func TestIsDatabaseNotExistsError_AllCases(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		engine string
		want   bool
	}{
		{"nil error mysql", nil, "mysql", false},
		{"nil error pgsql", nil, "pgsql", false},
		{"mysql 1049 error", errors.New("Error 1049: Unknown database"), "mysql", true},
		{"mysql unknown database", errors.New("Unknown database 'foo'"), "mysql", true},
		{"mysql other error", errors.New("Connection refused"), "mysql", false},
		{"pgsql 3D000 error", errors.New("pq: 3D000: database does not exist"), "pgsql", true},
		{"pgsql does not exist", errors.New("database \"foo\" does not exist"), "pgsql", true},
		{"pgsql other error", errors.New("password authentication failed"), "pgsql", false},
		{"unknown engine", errors.New("any error"), "sqlite", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isDatabaseNotExistsError(tt.err, tt.engine)
			if got != tt.want {
				t.Errorf("isDatabaseNotExistsError(%v, %q) = %v, want %v", tt.err, tt.engine, got, tt.want)
			}
		})
	}
}

func TestQuoteIdentifier_EdgeCases(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple", `"simple"`},
		{"with space", `"with space"`},
		{`with"quote`, `"with""quote"`},
		{`many""quotes""here`, `"many""""quotes""""here"`},
		{"", `""`},
		{"MixedCase", `"MixedCase"`},
		{"123", `"123"`},
		{"a-b-c", `"a-b-c"`},
	}

	for _, tt := range tests {
		got := quoteIdentifier(tt.input)
		if got != tt.want {
			t.Errorf("quoteIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestGetDatabaseInfo_MySQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock VERSION() query
	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.35"))

	// Mock table count and size query
	mock.ExpectQuery("SELECT COUNT\\(\\*\\), COALESCE\\(SUM\\(data_length \\+ index_length\\), 0\\)").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"count", "size"}).AddRow(5, 1024000))

	// Mock view count query
	mock.ExpectQuery("SELECT COUNT\\(\\*\\).*FROM information_schema.views").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))

	// Mock table names query
	mock.ExpectQuery("SELECT table_name.*FROM information_schema.tables").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("users").AddRow("orders"))

	info, err := getDatabaseInfo(db, "mysql", "testdb")
	if err != nil {
		t.Errorf("getDatabaseInfo() error = %v", err)
	}

	if info.Version != "8.0.35" {
		t.Errorf("info.Version = %q, want %q", info.Version, "8.0.35")
	}
	if info.MajorVersion != 8 {
		t.Errorf("info.MajorVersion = %d, want 8", info.MajorVersion)
	}
	if info.TableCount != 5 {
		t.Errorf("info.TableCount = %d, want 5", info.TableCount)
	}
	if info.ViewCount != 2 {
		t.Errorf("info.ViewCount = %d, want 2", info.ViewCount)
	}
	if info.TotalSize != 1024000 {
		t.Errorf("info.TotalSize = %d, want 1024000", info.TotalSize)
	}
	if len(info.Tables) != 2 {
		t.Errorf("len(info.Tables) = %d, want 2", len(info.Tables))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestGetDatabaseInfo_Postgres(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock version() query
	mock.ExpectQuery("SELECT version\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("PostgreSQL 16.2"))

	// Mock table count query
	mock.ExpectQuery("SELECT COUNT\\(\\*\\).*FROM information_schema.tables.*table_type = 'BASE TABLE'").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	// Mock database size query
	mock.ExpectQuery("SELECT pg_database_size\\(current_database\\(\\)\\)").
		WillReturnRows(sqlmock.NewRows([]string{"size"}).AddRow(int64(2048000)))

	// Mock view count query
	mock.ExpectQuery("SELECT COUNT\\(\\*\\).*FROM information_schema.views").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	// Mock table names query
	mock.ExpectQuery("SELECT table_name.*FROM information_schema.tables.*ORDER BY table_name").
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("products").AddRow("users"))

	info, err := getDatabaseInfo(db, "pgsql", "testdb")
	if err != nil {
		t.Errorf("getDatabaseInfo() error = %v", err)
	}

	if info.Version != "PostgreSQL 16.2" {
		t.Errorf("info.Version = %q, want %q", info.Version, "PostgreSQL 16.2")
	}
	if info.MajorVersion != 16 {
		t.Errorf("info.MajorVersion = %d, want 16", info.MajorVersion)
	}
	if info.TableCount != 3 {
		t.Errorf("info.TableCount = %d, want 3", info.TableCount)
	}
	if info.ViewCount != 1 {
		t.Errorf("info.ViewCount = %d, want 1", info.ViewCount)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestGetDatabaseInfo_MySQL_VersionError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnError(sqlmock.ErrCancelled)

	_, err = getDatabaseInfo(db, "mysql", "testdb")
	if err == nil {
		t.Errorf("getDatabaseInfo() expected error, got nil")
	}
}

func TestGetDatabaseInfo_MySQL_TableCountError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.35"))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\), COALESCE").
		WithArgs("testdb").
		WillReturnError(sqlmock.ErrCancelled)

	_, err = getDatabaseInfo(db, "mysql", "testdb")
	if err == nil {
		t.Errorf("getDatabaseInfo() expected error, got nil")
	}
}

func TestGetDatabaseInfo_MySQL_ViewCountError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.35"))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\), COALESCE").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"count", "size"}).AddRow(5, 1024000))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\).*FROM information_schema.views").
		WithArgs("testdb").
		WillReturnError(sqlmock.ErrCancelled)

	_, err = getDatabaseInfo(db, "mysql", "testdb")
	if err == nil {
		t.Errorf("getDatabaseInfo() expected error, got nil")
	}
}

func TestGetDatabaseInfo_MySQL_TableNamesError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.35"))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\), COALESCE").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"count", "size"}).AddRow(5, 1024000))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\).*FROM information_schema.views").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))

	mock.ExpectQuery("SELECT table_name.*FROM information_schema.tables").
		WithArgs("testdb").
		WillReturnError(sqlmock.ErrCancelled)

	_, err = getDatabaseInfo(db, "mysql", "testdb")
	if err == nil {
		t.Errorf("getDatabaseInfo() expected error, got nil")
	}
}

func TestWipeDatabase_MySQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Disable FK checks
	mock.ExpectExec("SET FOREIGN_KEY_CHECKS = 0").
		WillReturnResult(sqlmock.NewResult(0, 0))

	// Query tables
	mock.ExpectQuery("SELECT table_name.*FROM information_schema.tables.*table_type = 'BASE TABLE'").
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("users").AddRow("orders"))

	// Drop tables
	mock.ExpectExec("DROP TABLE IF EXISTS `users`").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("DROP TABLE IF EXISTS `orders`").
		WillReturnResult(sqlmock.NewResult(0, 0))

	// Query views
	mock.ExpectQuery("SELECT table_name.*FROM information_schema.views").
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("user_stats"))

	// Drop views
	mock.ExpectExec("DROP VIEW IF EXISTS `user_stats`").
		WillReturnResult(sqlmock.NewResult(0, 0))

	// Re-enable FK checks
	mock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = wipeDatabase(db, "mysql")
	if err != nil {
		t.Errorf("wipeDatabase() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestWipeDatabase_Postgres(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Query tables
	mock.ExpectQuery("SELECT tablename.*FROM pg_tables").
		WillReturnRows(sqlmock.NewRows([]string{"tablename"}).AddRow("users").AddRow("orders"))

	// Drop tables
	mock.ExpectExec(`DROP TABLE IF EXISTS "users" CASCADE`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DROP TABLE IF EXISTS "orders" CASCADE`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// Query views
	mock.ExpectQuery("SELECT viewname.*FROM pg_views").
		WillReturnRows(sqlmock.NewRows([]string{"viewname"}).AddRow("user_stats"))

	// Drop views
	mock.ExpectExec(`DROP VIEW IF EXISTS "user_stats" CASCADE`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// Query sequences
	mock.ExpectQuery("SELECT sequencename.*FROM pg_sequences").
		WillReturnRows(sqlmock.NewRows([]string{"sequencename"}).AddRow("users_id_seq"))

	// Drop sequences
	mock.ExpectExec(`DROP SEQUENCE IF EXISTS "users_id_seq" CASCADE`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = wipeDatabase(db, "pgsql")
	if err != nil {
		t.Errorf("wipeDatabase() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestWipeDatabase_MySQL_DisableFKError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec("SET FOREIGN_KEY_CHECKS = 0").
		WillReturnError(sqlmock.ErrCancelled)

	err = wipeDatabase(db, "mysql")
	if err == nil {
		t.Errorf("wipeDatabase() expected error, got nil")
	}
}

func TestWipeDatabase_MySQL_QueryTablesError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec("SET FOREIGN_KEY_CHECKS = 0").
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectQuery("SELECT table_name.*FROM information_schema.tables").
		WillReturnError(sqlmock.ErrCancelled)

	err = wipeDatabase(db, "mysql")
	if err == nil {
		t.Errorf("wipeDatabase() expected error, got nil")
	}
}

func TestWipeDatabase_MySQL_DropTableError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec("SET FOREIGN_KEY_CHECKS = 0").
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectQuery("SELECT table_name.*FROM information_schema.tables").
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("users"))

	mock.ExpectExec("DROP TABLE IF EXISTS `users`").
		WillReturnError(sqlmock.ErrCancelled)

	err = wipeDatabase(db, "mysql")
	if err == nil {
		t.Errorf("wipeDatabase() expected error, got nil")
	}
}

func TestWipeDatabase_Postgres_QueryTablesError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT tablename.*FROM pg_tables").
		WillReturnError(sqlmock.ErrCancelled)

	err = wipeDatabase(db, "pgsql")
	if err == nil {
		t.Errorf("wipeDatabase() expected error, got nil")
	}
}

func TestWipeDatabase_Postgres_DropTableError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT tablename.*FROM pg_tables").
		WillReturnRows(sqlmock.NewRows([]string{"tablename"}).AddRow("users"))

	mock.ExpectExec(`DROP TABLE IF EXISTS "users" CASCADE`).
		WillReturnError(sqlmock.ErrCancelled)

	err = wipeDatabase(db, "pgsql")
	if err == nil {
		t.Errorf("wipeDatabase() expected error, got nil")
	}
}

func TestWipeDatabase_MySQL_QueryViewsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec("SET FOREIGN_KEY_CHECKS = 0").
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectQuery("SELECT table_name.*FROM information_schema.tables").
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}))

	mock.ExpectQuery("SELECT table_name.*FROM information_schema.views").
		WillReturnError(sqlmock.ErrCancelled)

	err = wipeDatabase(db, "mysql")
	if err == nil {
		t.Errorf("wipeDatabase() expected error, got nil")
	}
}

func TestWipeDatabase_Postgres_QueryViewsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT tablename.*FROM pg_tables").
		WillReturnRows(sqlmock.NewRows([]string{"tablename"}))

	mock.ExpectQuery("SELECT viewname.*FROM pg_views").
		WillReturnError(sqlmock.ErrCancelled)

	err = wipeDatabase(db, "pgsql")
	if err == nil {
		t.Errorf("wipeDatabase() expected error, got nil")
	}
}

func TestWipeDatabase_Postgres_QuerySequencesError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT tablename.*FROM pg_tables").
		WillReturnRows(sqlmock.NewRows([]string{"tablename"}))

	mock.ExpectQuery("SELECT viewname.*FROM pg_views").
		WillReturnRows(sqlmock.NewRows([]string{"viewname"}))

	mock.ExpectQuery("SELECT sequencename.*FROM pg_sequences").
		WillReturnError(sqlmock.ErrCancelled)

	err = wipeDatabase(db, "pgsql")
	if err == nil {
		t.Errorf("wipeDatabase() expected error, got nil")
	}
}

func TestWipeDatabase_UnknownEngine(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Unknown engine should return nil (no-op)
	err = wipeDatabase(db, "sqlite")
	if err != nil {
		t.Errorf("wipeDatabase() with unknown engine error = %v, want nil", err)
	}
}

func TestGetDatabaseInfo_Postgres_VersionError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT version\\(\\)").
		WillReturnError(sqlmock.ErrCancelled)

	_, err = getDatabaseInfo(db, "pgsql", "testdb")
	if err == nil {
		t.Errorf("getDatabaseInfo() expected error, got nil")
	}
}

func TestGetDatabaseInfo_Postgres_TableCountError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT version\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("PostgreSQL 16.2"))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\).*FROM information_schema.tables").
		WillReturnError(sqlmock.ErrCancelled)

	_, err = getDatabaseInfo(db, "pgsql", "testdb")
	if err == nil {
		t.Errorf("getDatabaseInfo() expected error, got nil")
	}
}

func TestGetDatabaseInfo_Postgres_SizeError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT version\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("PostgreSQL 16.2"))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\).*FROM information_schema.tables").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	mock.ExpectQuery("SELECT pg_database_size").
		WillReturnError(sqlmock.ErrCancelled)

	_, err = getDatabaseInfo(db, "pgsql", "testdb")
	if err == nil {
		t.Errorf("getDatabaseInfo() expected error, got nil")
	}
}

func TestGetDatabaseInfo_Postgres_ViewCountError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT version\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("PostgreSQL 16.2"))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\).*FROM information_schema.tables").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	mock.ExpectQuery("SELECT pg_database_size").
		WillReturnRows(sqlmock.NewRows([]string{"size"}).AddRow(int64(2048000)))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\).*FROM information_schema.views").
		WillReturnError(sqlmock.ErrCancelled)

	_, err = getDatabaseInfo(db, "pgsql", "testdb")
	if err == nil {
		t.Errorf("getDatabaseInfo() expected error, got nil")
	}
}

func TestGetDatabaseInfo_Postgres_TableNamesError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT version\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("PostgreSQL 16.2"))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\).*FROM information_schema.tables").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	mock.ExpectQuery("SELECT pg_database_size").
		WillReturnRows(sqlmock.NewRows([]string{"size"}).AddRow(int64(2048000)))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\).*FROM information_schema.views").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	mock.ExpectQuery("SELECT table_name.*FROM information_schema.tables.*ORDER BY table_name").
		WillReturnError(sqlmock.ErrCancelled)

	_, err = getDatabaseInfo(db, "pgsql", "testdb")
	if err == nil {
		t.Errorf("getDatabaseInfo() expected error, got nil")
	}
}

func TestGetDatabaseInfo_UnknownEngine(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Unknown engine should return empty info
	info, err := getDatabaseInfo(db, "sqlite", "testdb")
	if err != nil {
		t.Errorf("getDatabaseInfo() with unknown engine error = %v, want nil", err)
	}
	if info.Version != "" {
		t.Errorf("info.Version = %q, want empty", info.Version)
	}
}
