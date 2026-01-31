package data

import (
	"fmt"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/DGarbs51/lcmigrate/internal/dialect"
	"github.com/DGarbs51/lcmigrate/internal/schema"
)

func TestNewTransferer(t *testing.T) {
	tests := []struct {
		engine   string
		wantNil  bool
		wantType string
	}{
		{"mysql", false, "*data.MySQLTransferer"},
		{"pgsql", false, "*data.PostgresTransferer"},
		{"unknown", true, ""},
		{"", true, ""},
	}

	for _, tt := range tests {
		tr := NewTransferer(tt.engine)
		if tt.wantNil {
			if tr != nil {
				t.Errorf("NewTransferer(%q) = %v, want nil", tt.engine, tr)
			}
		} else {
			if tr == nil {
				t.Errorf("NewTransferer(%q) = nil, want non-nil", tt.engine)
			}
		}
	}
}

func TestBaseTransferer_DisableForeignKeyChecks_MySQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	bt := &BaseTransferer{Dialect: &dialect.MySQLDialect{}}

	mock.ExpectExec("SET FOREIGN_KEY_CHECKS = 0").WillReturnResult(sqlmock.NewResult(0, 0))

	err = bt.DisableForeignKeyChecks(db)
	if err != nil {
		t.Errorf("DisableForeignKeyChecks() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestBaseTransferer_EnableForeignKeyChecks_MySQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	bt := &BaseTransferer{Dialect: &dialect.MySQLDialect{}}

	mock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))

	err = bt.EnableForeignKeyChecks(db)
	if err != nil {
		t.Errorf("EnableForeignKeyChecks() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestBaseTransferer_DisableForeignKeyChecks_Postgres(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	bt := &BaseTransferer{Dialect: &dialect.PostgresDialect{}}

	mock.ExpectExec("SET session_replication_role = replica").WillReturnResult(sqlmock.NewResult(0, 0))

	err = bt.DisableForeignKeyChecks(db)
	if err != nil {
		t.Errorf("DisableForeignKeyChecks() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestBaseTransferer_EnableForeignKeyChecks_Postgres(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	bt := &BaseTransferer{Dialect: &dialect.PostgresDialect{}}

	mock.ExpectExec("SET session_replication_role = DEFAULT").WillReturnResult(sqlmock.NewResult(0, 0))

	err = bt.EnableForeignKeyChecks(db)
	if err != nil {
		t.Errorf("EnableForeignKeyChecks() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestBaseTransferer_EstimateRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	bt := &BaseTransferer{Dialect: &dialect.MySQLDialect{}}

	rows := sqlmock.NewRows([]string{"count"}).AddRow(int64(1000))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `users`").WillReturnRows(rows)

	count, err := bt.EstimateRows(db, "users")
	if err != nil {
		t.Errorf("EstimateRows() error = %v", err)
	}
	if count != 1000 {
		t.Errorf("EstimateRows() = %d, want 1000", count)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestBaseTransferer_GetColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	bt := &BaseTransferer{Dialect: &dialect.MySQLDialect{}}

	rows := sqlmock.NewRows([]string{"id", "name", "email"})
	mock.ExpectQuery("SELECT \\* FROM `users` LIMIT 0").WillReturnRows(rows)

	columns, err := bt.GetColumns(db, "users")
	if err != nil {
		t.Errorf("GetColumns() error = %v", err)
	}
	if len(columns) != 3 {
		t.Errorf("GetColumns() returned %d columns, want 3", len(columns))
	}
	expected := []string{"id", "name", "email"}
	for i, col := range columns {
		if col != expected[i] {
			t.Errorf("columns[%d] = %q, want %q", i, col, expected[i])
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestBaseTransferer_InsertBatch_MySQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	bt := &BaseTransferer{Dialect: &dialect.MySQLDialect{}}

	columns := []string{"id", "name"}
	batch := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}

	// MySQL uses ? placeholders
	mock.ExpectExec("INSERT INTO `users` \\(`id`, `name`\\) VALUES \\(\\?, \\?\\), \\(\\?, \\?\\)").
		WithArgs(1, "Alice", 2, "Bob").
		WillReturnResult(sqlmock.NewResult(2, 2))

	err = bt.InsertBatch(db, "users", columns, batch)
	if err != nil {
		t.Errorf("InsertBatch() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestBaseTransferer_InsertBatch_Postgres(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	bt := &BaseTransferer{Dialect: &dialect.PostgresDialect{}}

	columns := []string{"id", "name"}
	batch := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}

	// PostgreSQL uses $1, $2, etc. placeholders
	mock.ExpectExec(`INSERT INTO "users" \("id", "name"\) VALUES \(\$1, \$2\), \(\$3, \$4\)`).
		WithArgs(1, "Alice", 2, "Bob").
		WillReturnResult(sqlmock.NewResult(2, 2))

	err = bt.InsertBatch(db, "users", columns, batch)
	if err != nil {
		t.Errorf("InsertBatch() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestBaseTransferer_InsertBatch_EmptyBatch(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	bt := &BaseTransferer{Dialect: &dialect.MySQLDialect{}}

	// Empty batch should return nil without executing anything
	err = bt.InsertBatch(db, "users", []string{"id"}, [][]interface{}{})
	if err != nil {
		t.Errorf("InsertBatch() with empty batch error = %v", err)
	}
}

func TestBaseTransferer_TransferTable_DryRun(t *testing.T) {
	sourceDB, sourceMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	bt := &BaseTransferer{Dialect: &dialect.MySQLDialect{}}

	// Mock GetColumns
	colRows := sqlmock.NewRows([]string{"id", "name"})
	sourceMock.ExpectQuery("SELECT \\* FROM `users` LIMIT 0").WillReturnRows(colRows)

	// Mock EstimateRows
	countRows := sqlmock.NewRows([]string{"count"}).AddRow(int64(500))
	sourceMock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `users`").WillReturnRows(countRows)

	table := schema.TableSchema{Name: "users"}

	stats, err := bt.TransferTable(sourceDB, destDB, table, 100, true, nil)
	if err != nil {
		t.Errorf("TransferTable() error = %v", err)
	}
	if stats.TableName != "users" {
		t.Errorf("stats.TableName = %q, want %q", stats.TableName, "users")
	}
	if stats.RowsCopied != 500 {
		t.Errorf("stats.RowsCopied = %d, want 500", stats.RowsCopied)
	}

	if err := sourceMock.ExpectationsWereMet(); err != nil {
		t.Errorf("source expectations not met: %v", err)
	}
}

func TestBaseTransferer_TransferTable_EmptyTable(t *testing.T) {
	sourceDB, sourceMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	bt := &BaseTransferer{Dialect: &dialect.MySQLDialect{}}

	// Mock GetColumns returning empty
	colRows := sqlmock.NewRows([]string{})
	sourceMock.ExpectQuery("SELECT \\* FROM `empty_table` LIMIT 0").WillReturnRows(colRows)

	table := schema.TableSchema{Name: "empty_table"}

	stats, err := bt.TransferTable(sourceDB, destDB, table, 100, false, nil)
	if err != nil {
		t.Errorf("TransferTable() error = %v", err)
	}
	if stats.RowsCopied != 0 {
		t.Errorf("stats.RowsCopied = %d, want 0", stats.RowsCopied)
	}

	if err := sourceMock.ExpectationsWereMet(); err != nil {
		t.Errorf("source expectations not met: %v", err)
	}
}

func TestBaseTransferer_TransferTable_WithData(t *testing.T) {
	sourceDB, sourceMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, destMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	bt := &BaseTransferer{Dialect: &dialect.MySQLDialect{}}

	// Mock GetColumns
	colRows := sqlmock.NewRows([]string{"id", "name"})
	sourceMock.ExpectQuery("SELECT \\* FROM `users` LIMIT 0").WillReturnRows(colRows)

	// Mock EstimateRows
	countRows := sqlmock.NewRows([]string{"count"}).AddRow(int64(2))
	sourceMock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `users`").WillReturnRows(countRows)

	// Mock batch read
	dataRows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "Alice").
		AddRow(2, "Bob")
	sourceMock.ExpectQuery("SELECT `id`, `name` FROM `users` LIMIT 100 OFFSET 0").WillReturnRows(dataRows)

	// Mock insert
	destMock.ExpectExec("INSERT INTO `users` \\(`id`, `name`\\) VALUES \\(\\?, \\?\\), \\(\\?, \\?\\)").
		WithArgs(1, "Alice", 2, "Bob").
		WillReturnResult(sqlmock.NewResult(2, 2))

	table := schema.TableSchema{Name: "users"}

	var progressCalls []int64
	progressFn := func(rows int64) {
		progressCalls = append(progressCalls, rows)
	}

	stats, err := bt.TransferTable(sourceDB, destDB, table, 100, false, progressFn)
	if err != nil {
		t.Errorf("TransferTable() error = %v", err)
	}
	if stats.RowsCopied != 2 {
		t.Errorf("stats.RowsCopied = %d, want 2", stats.RowsCopied)
	}
	if len(progressCalls) != 1 || progressCalls[0] != 2 {
		t.Errorf("progressFn calls = %v, want [2]", progressCalls)
	}

	if err := sourceMock.ExpectationsWereMet(); err != nil {
		t.Errorf("source expectations not met: %v", err)
	}
	if err := destMock.ExpectationsWereMet(); err != nil {
		t.Errorf("dest expectations not met: %v", err)
	}
}

func TestBaseTransferer_TransferTable_MultipleBatches(t *testing.T) {
	sourceDB, sourceMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, destMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	bt := &BaseTransferer{Dialect: &dialect.MySQLDialect{}}

	// Mock GetColumns
	colRows := sqlmock.NewRows([]string{"id", "name"})
	sourceMock.ExpectQuery("SELECT \\* FROM `users` LIMIT 0").WillReturnRows(colRows)

	// Mock EstimateRows
	countRows := sqlmock.NewRows([]string{"count"}).AddRow(int64(3))
	sourceMock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `users`").WillReturnRows(countRows)

	// First batch read (batch size 2)
	batch1 := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "Alice").
		AddRow(2, "Bob")
	sourceMock.ExpectQuery("SELECT `id`, `name` FROM `users` LIMIT 2 OFFSET 0").WillReturnRows(batch1)

	// First batch insert
	destMock.ExpectExec("INSERT INTO `users` \\(`id`, `name`\\) VALUES \\(\\?, \\?\\), \\(\\?, \\?\\)").
		WithArgs(1, "Alice", 2, "Bob").
		WillReturnResult(sqlmock.NewResult(2, 2))

	// Second batch read
	batch2 := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(3, "Charlie")
	sourceMock.ExpectQuery("SELECT `id`, `name` FROM `users` LIMIT 2 OFFSET 2").WillReturnRows(batch2)

	// Second batch insert
	destMock.ExpectExec("INSERT INTO `users` \\(`id`, `name`\\) VALUES \\(\\?, \\?\\)").
		WithArgs(3, "Charlie").
		WillReturnResult(sqlmock.NewResult(1, 1))

	table := schema.TableSchema{Name: "users"}

	stats, err := bt.TransferTable(sourceDB, destDB, table, 2, false, nil)
	if err != nil {
		t.Errorf("TransferTable() error = %v", err)
	}
	if stats.RowsCopied != 3 {
		t.Errorf("stats.RowsCopied = %d, want 3", stats.RowsCopied)
	}

	if err := sourceMock.ExpectationsWereMet(); err != nil {
		t.Errorf("source expectations not met: %v", err)
	}
	if err := destMock.ExpectationsWereMet(); err != nil {
		t.Errorf("dest expectations not met: %v", err)
	}
}

func TestBaseTransferer_TransferTable_GetColumnsError(t *testing.T) {
	sourceDB, sourceMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	bt := &BaseTransferer{Dialect: &dialect.MySQLDialect{}}

	// Mock GetColumns returning error
	sourceMock.ExpectQuery("SELECT \\* FROM `users` LIMIT 0").
		WillReturnError(fmt.Errorf("table not found"))

	table := schema.TableSchema{Name: "users"}

	_, err = bt.TransferTable(sourceDB, destDB, table, 100, false, nil)
	if err == nil {
		t.Errorf("TransferTable() expected error, got nil")
	}
	if !strings.Contains(err.Error(), "failed to get columns") {
		t.Errorf("TransferTable() error = %v, want to contain 'failed to get columns'", err)
	}
}

func TestBaseTransferer_TransferTable_EstimateRowsError(t *testing.T) {
	sourceDB, sourceMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	bt := &BaseTransferer{Dialect: &dialect.MySQLDialect{}}

	// Mock GetColumns
	colRows := sqlmock.NewRows([]string{"id", "name"})
	sourceMock.ExpectQuery("SELECT \\* FROM `users` LIMIT 0").WillReturnRows(colRows)

	// Mock EstimateRows returning error (for dry run path)
	sourceMock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `users`").
		WillReturnError(fmt.Errorf("permission denied"))

	table := schema.TableSchema{Name: "users"}

	_, err = bt.TransferTable(sourceDB, destDB, table, 100, true, nil)
	if err == nil {
		t.Errorf("TransferTable() expected error, got nil")
	}
	if !strings.Contains(err.Error(), "failed to estimate rows") {
		t.Errorf("TransferTable() error = %v, want to contain 'failed to estimate rows'", err)
	}
}

func TestNewMySQLTransferer(t *testing.T) {
	tr := NewMySQLTransferer()
	if tr == nil {
		t.Errorf("NewMySQLTransferer() = nil, want non-nil")
	}
	if tr.Dialect == nil {
		t.Errorf("NewMySQLTransferer().Dialect = nil, want non-nil")
	}
	// Verify it's a MySQL dialect by checking quote style
	if tr.Dialect.QuoteIdentifier("test") != "`test`" {
		t.Errorf("NewMySQLTransferer().Dialect.QuoteIdentifier() = %q, want backtick quoting", tr.Dialect.QuoteIdentifier("test"))
	}
}

func TestNewPostgresTransferer(t *testing.T) {
	tr := NewPostgresTransferer()
	if tr == nil {
		t.Errorf("NewPostgresTransferer() = nil, want non-nil")
	}
	if tr.Dialect == nil {
		t.Errorf("NewPostgresTransferer().Dialect = nil, want non-nil")
	}
	// Verify it's a Postgres dialect by checking quote style
	if tr.Dialect.QuoteIdentifier("test") != `"test"` {
		t.Errorf("NewPostgresTransferer().Dialect.QuoteIdentifier() = %q, want double-quote quoting", tr.Dialect.QuoteIdentifier("test"))
	}
}

func TestBaseTransferer_EstimateRows_Postgres(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	bt := &BaseTransferer{Dialect: &dialect.PostgresDialect{}}

	rows := sqlmock.NewRows([]string{"count"}).AddRow(int64(500))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM "users"`).WillReturnRows(rows)

	count, err := bt.EstimateRows(db, "users")
	if err != nil {
		t.Errorf("EstimateRows() error = %v", err)
	}
	if count != 500 {
		t.Errorf("EstimateRows() = %d, want 500", count)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestBaseTransferer_GetColumns_Postgres(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	bt := &BaseTransferer{Dialect: &dialect.PostgresDialect{}}

	rows := sqlmock.NewRows([]string{"id", "email", "created_at"})
	mock.ExpectQuery(`SELECT \* FROM "users" LIMIT 0`).WillReturnRows(rows)

	columns, err := bt.GetColumns(db, "users")
	if err != nil {
		t.Errorf("GetColumns() error = %v", err)
	}
	if len(columns) != 3 {
		t.Errorf("GetColumns() returned %d columns, want 3", len(columns))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}
