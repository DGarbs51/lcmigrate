package schema

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/DGarbs51/lcmigrate/internal/dialect"
)

func TestNewExtractor(t *testing.T) {
	tests := []struct {
		engine  string
		wantNil bool
	}{
		{"mysql", false},
		{"pgsql", false},
		{"unknown", true},
		{"", true},
	}

	for _, tt := range tests {
		ext := NewExtractor(tt.engine)
		if tt.wantNil {
			if ext != nil {
				t.Errorf("NewExtractor(%q) = %v, want nil", tt.engine, ext)
			}
		} else {
			if ext == nil {
				t.Errorf("NewExtractor(%q) = nil, want non-nil", tt.engine)
			}
		}
	}
}

func TestNewApplier(t *testing.T) {
	tests := []struct {
		engine  string
		wantNil bool
	}{
		{"mysql", false},
		{"pgsql", false},
		{"unknown", true},
		{"", true},
	}

	for _, tt := range tests {
		app := NewApplier(tt.engine)
		if tt.wantNil {
			if app != nil {
				t.Errorf("NewApplier(%q) = %v, want nil", tt.engine, app)
			}
		} else {
			if app == nil {
				t.Errorf("NewApplier(%q) = nil, want non-nil", tt.engine)
			}
		}
	}
}

func TestBaseApplier_CreateIndex(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	applier := &BaseApplier{Dialect: &dialect.MySQLDialect{}}

	index := IndexDef{
		Name:       "idx_users_email",
		Columns:    []string{"email"},
		IsUnique:   true,
		CreateStmt: "CREATE UNIQUE INDEX `idx_users_email` ON `users` (`email`)",
	}

	mock.ExpectExec("CREATE UNIQUE INDEX `idx_users_email` ON `users` \\(`email`\\)").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = applier.CreateIndex(db, index)
	if err != nil {
		t.Errorf("CreateIndex() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestBaseApplier_CreateIndex_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	applier := &BaseApplier{Dialect: &dialect.MySQLDialect{}}

	index := IndexDef{
		Name:       "idx_test",
		CreateStmt: "CREATE INDEX idx_test ON test (col)",
	}

	mock.ExpectExec("CREATE INDEX idx_test ON test \\(col\\)").
		WillReturnError(sqlmock.ErrCancelled)

	err = applier.CreateIndex(db, index)
	if err == nil {
		t.Errorf("CreateIndex() expected error, got nil")
	}
}

func TestBaseApplier_CreateForeignKey(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	applier := &BaseApplier{Dialect: &dialect.MySQLDialect{}}

	fk := ForeignKeyDef{
		Name:           "fk_orders_user_id",
		Columns:        []string{"user_id"},
		RefTable:       "users",
		RefColumns:     []string{"id"},
		OnDelete:       "CASCADE",
		OnUpdate:       "NO ACTION",
		ConstraintStmt: "ALTER TABLE `orders` ADD CONSTRAINT `fk_orders_user_id` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE",
	}

	mock.ExpectExec("ALTER TABLE `orders` ADD CONSTRAINT `fk_orders_user_id` FOREIGN KEY \\(`user_id`\\) REFERENCES `users` \\(`id`\\) ON DELETE CASCADE").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = applier.CreateForeignKey(db, fk)
	if err != nil {
		t.Errorf("CreateForeignKey() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestBaseApplier_CreateForeignKey_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	applier := &BaseApplier{Dialect: &dialect.MySQLDialect{}}

	fk := ForeignKeyDef{
		Name:           "fk_test",
		ConstraintStmt: "ALTER TABLE test ADD CONSTRAINT fk_test FOREIGN KEY (id) REFERENCES other (id)",
	}

	mock.ExpectExec("ALTER TABLE test ADD CONSTRAINT fk_test FOREIGN KEY \\(id\\) REFERENCES other \\(id\\)").
		WillReturnError(sqlmock.ErrCancelled)

	err = applier.CreateForeignKey(db, fk)
	if err == nil {
		t.Errorf("CreateForeignKey() expected error, got nil")
	}
}

func TestBaseApplier_CreateView(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	applier := &BaseApplier{Dialect: &dialect.MySQLDialect{}}

	view := ViewDef{
		Name:       "user_stats",
		CreateStmt: "CREATE VIEW user_stats AS SELECT id, COUNT(*) as count FROM users GROUP BY id",
	}

	mock.ExpectExec("CREATE VIEW user_stats AS SELECT id, COUNT\\(\\*\\) as count FROM users GROUP BY id").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = applier.CreateView(db, view)
	if err != nil {
		t.Errorf("CreateView() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestBaseApplier_CreateView_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	applier := &BaseApplier{Dialect: &dialect.MySQLDialect{}}

	view := ViewDef{
		Name:       "bad_view",
		CreateStmt: "CREATE VIEW bad_view AS SELECT invalid FROM missing",
	}

	mock.ExpectExec("CREATE VIEW bad_view AS SELECT invalid FROM missing").
		WillReturnError(sqlmock.ErrCancelled)

	err = applier.CreateView(db, view)
	if err == nil {
		t.Errorf("CreateView() expected error, got nil")
	}
}
