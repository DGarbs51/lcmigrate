package data

import (
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
