package schema

import (
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestNewMySQLExtractor(t *testing.T) {
	ext := NewMySQLExtractor()
	if ext == nil {
		t.Errorf("NewMySQLExtractor() = nil, want non-nil")
	}
	if ext.Dialect == nil {
		t.Errorf("NewMySQLExtractor().Dialect = nil, want non-nil")
	}
}

func TestNewMySQLApplier(t *testing.T) {
	app := NewMySQLApplier()
	if app == nil {
		t.Errorf("NewMySQLApplier() = nil, want non-nil")
	}
	if app.Dialect == nil {
		t.Errorf("NewMySQLApplier().Dialect = nil, want non-nil")
	}
}

func TestMySQLExtractor_ExtractSequences(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	ext := NewMySQLExtractor()

	// MySQL doesn't have sequences, should return nil, nil
	sequences, err := ext.ExtractSequences(db, "testdb")
	if err != nil {
		t.Errorf("ExtractSequences() error = %v, want nil", err)
	}
	if sequences != nil {
		t.Errorf("ExtractSequences() = %v, want nil", sequences)
	}
}

func TestMySQLApplier_CreateSequence(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	app := NewMySQLApplier()
	seq := SequenceDef{Name: "test_seq"}

	// Should be no-op for MySQL
	err = app.CreateSequence(db, seq)
	if err != nil {
		t.Errorf("CreateSequence() error = %v, want nil", err)
	}
}

func TestMySQLApplier_SetSequenceValue(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	app := NewMySQLApplier()
	seq := SequenceDef{Name: "test_seq", CurrentVal: 100}

	// Should be no-op for MySQL
	err = app.SetSequenceValue(db, seq)
	if err != nil {
		t.Errorf("SetSequenceValue() error = %v, want nil", err)
	}
}

func TestMySQLApplier_CreateTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	app := NewMySQLApplier()

	// Test with a CREATE TABLE statement without foreign keys
	table := TableSchema{
		Name:       "users",
		CreateStmt: "CREATE TABLE `users` (`id` int NOT NULL AUTO_INCREMENT, `name` varchar(255), PRIMARY KEY (`id`))",
	}

	mock.ExpectExec("CREATE TABLE `users` \\(`id` int NOT NULL AUTO_INCREMENT, `name` varchar\\(255\\), PRIMARY KEY \\(`id`\\)\\)").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = app.CreateTable(db, table)
	if err != nil {
		t.Errorf("CreateTable() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestMySQLApplier_CreateTable_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	app := NewMySQLApplier()
	table := TableSchema{
		Name:       "bad_table",
		CreateStmt: "CREATE TABLE bad_table (id int)",
	}

	mock.ExpectExec("CREATE TABLE bad_table \\(id int\\)").
		WillReturnError(sqlmock.ErrCancelled)

	err = app.CreateTable(db, table)
	if err == nil {
		t.Errorf("CreateTable() expected error, got nil")
	}
}

func TestRemoveForeignKeysFromCreateTable(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		shouldRemove bool // whether the FK constraint should be removed
	}{
		{
			name:         "no foreign keys",
			input:        "CREATE TABLE `users` (`id` int, PRIMARY KEY (`id`))",
			shouldRemove: false,
		},
		{
			name:         "with named constraint",
			input:        "CREATE TABLE `orders` (`id` int, `user_id` int, CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`))",
			shouldRemove: true,
		},
		{
			name:         "with on delete cascade",
			input:        "CREATE TABLE `orders` (`id` int, `user_id` int, CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE)",
			shouldRemove: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := removeForeignKeysFromCreateTable(tt.input)
			hasForeignKey := strings.Contains(got, "FOREIGN KEY") || strings.Contains(got, "CONSTRAINT")
			if tt.shouldRemove && hasForeignKey {
				t.Errorf("removeForeignKeysFromCreateTable() still contains FK constraint: %q", got)
			}
			if !tt.shouldRemove && got != tt.input {
				t.Errorf("removeForeignKeysFromCreateTable() modified input when it shouldn't: got %q, want %q", got, tt.input)
			}
		})
	}
}

func TestExtractViewDependencies(t *testing.T) {
	tests := []struct {
		name    string
		viewDef string
		want    int // number of dependencies
	}{
		{
			name:    "simple FROM",
			viewDef: "SELECT * FROM users",
			want:    1,
		},
		{
			name:    "with JOIN",
			viewDef: "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
			want:    2,
		},
		{
			name:    "multiple JOINs",
			viewDef: "SELECT * FROM users JOIN orders ON users.id = orders.user_id LEFT JOIN products ON orders.product_id = products.id",
			want:    3,
		},
		{
			name:    "backtick quoted tables",
			viewDef: "SELECT * FROM `users` JOIN `orders` ON `users`.id = `orders`.user_id",
			want:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := extractViewDependencies(tt.viewDef)
			if len(deps) != tt.want {
				t.Errorf("extractViewDependencies() returned %d deps, want %d", len(deps), tt.want)
			}
		})
	}
}

func TestSortViewsByDependency(t *testing.T) {
	// Test case: view2 depends on view1
	views := []ViewDef{
		{Name: "view2", Dependencies: []string{"view1"}},
		{Name: "view1", Dependencies: []string{}},
	}

	sorted := sortViewsByDependency(views)

	// view1 should come before view2
	if len(sorted) != 2 {
		t.Errorf("sortViewsByDependency() returned %d views, want 2", len(sorted))
	}
	if sorted[0].Name != "view1" {
		t.Errorf("sorted[0].Name = %q, want %q", sorted[0].Name, "view1")
	}
	if sorted[1].Name != "view2" {
		t.Errorf("sorted[1].Name = %q, want %q", sorted[1].Name, "view2")
	}
}

func TestSortViewsByDependency_NoDependencies(t *testing.T) {
	views := []ViewDef{
		{Name: "view1", Dependencies: []string{}},
		{Name: "view2", Dependencies: []string{}},
	}

	sorted := sortViewsByDependency(views)
	if len(sorted) != 2 {
		t.Errorf("sortViewsByDependency() returned %d views, want 2", len(sorted))
	}
}

func TestSortViewsByDependency_ChainedDependencies(t *testing.T) {
	// view3 -> view2 -> view1
	views := []ViewDef{
		{Name: "view3", Dependencies: []string{"view2"}},
		{Name: "view1", Dependencies: []string{}},
		{Name: "view2", Dependencies: []string{"view1"}},
	}

	sorted := sortViewsByDependency(views)

	if len(sorted) != 3 {
		t.Errorf("sortViewsByDependency() returned %d views, want 3", len(sorted))
	}

	// Order should be: view1, view2, view3
	order := map[string]int{}
	for i, v := range sorted {
		order[v.Name] = i
	}

	if order["view1"] > order["view2"] {
		t.Errorf("view1 should come before view2")
	}
	if order["view2"] > order["view3"] {
		t.Errorf("view2 should come before view3")
	}
}

func TestMySQLExtractor_buildCreateIndexStmt(t *testing.T) {
	ext := NewMySQLExtractor()

	tests := []struct {
		name      string
		tableName string
		idx       IndexDef
		want      string
	}{
		{
			name:      "non-unique index",
			tableName: "users",
			idx: IndexDef{
				Name:     "idx_name",
				Columns:  []string{"name"},
				IsUnique: false,
			},
			want: "CREATE INDEX `idx_name` ON `users` (`name`)",
		},
		{
			name:      "unique index",
			tableName: "users",
			idx: IndexDef{
				Name:     "idx_email",
				Columns:  []string{"email"},
				IsUnique: true,
			},
			want: "CREATE UNIQUE INDEX `idx_email` ON `users` (`email`)",
		},
		{
			name:      "multi-column index",
			tableName: "orders",
			idx: IndexDef{
				Name:     "idx_user_date",
				Columns:  []string{"user_id", "order_date"},
				IsUnique: false,
			},
			want: "CREATE INDEX `idx_user_date` ON `orders` (`user_id`, `order_date`)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ext.buildCreateIndexStmt(tt.tableName, tt.idx)
			if got != tt.want {
				t.Errorf("buildCreateIndexStmt() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMySQLExtractor_buildAddForeignKeyStmt(t *testing.T) {
	ext := NewMySQLExtractor()

	tests := []struct {
		name      string
		tableName string
		fk        ForeignKeyDef
		wantPart  string
	}{
		{
			name:      "simple FK",
			tableName: "orders",
			fk: ForeignKeyDef{
				Name:       "fk_orders_user",
				Columns:    []string{"user_id"},
				RefTable:   "users",
				RefColumns: []string{"id"},
				OnDelete:   "RESTRICT",
				OnUpdate:   "RESTRICT",
			},
			wantPart: "ALTER TABLE `orders` ADD CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)",
		},
		{
			name:      "FK with CASCADE",
			tableName: "orders",
			fk: ForeignKeyDef{
				Name:       "fk_orders_user",
				Columns:    []string{"user_id"},
				RefTable:   "users",
				RefColumns: []string{"id"},
				OnDelete:   "CASCADE",
				OnUpdate:   "SET NULL",
			},
			wantPart: "ON DELETE CASCADE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ext.buildAddForeignKeyStmt(tt.tableName, tt.fk)
			if got == "" {
				t.Errorf("buildAddForeignKeyStmt() returned empty string")
			}
			// Just check that the expected part is present
			if tt.wantPart != "" && !contains(got, tt.wantPart) {
				t.Errorf("buildAddForeignKeyStmt() = %q, want to contain %q", got, tt.wantPart)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestMySQLExtractor_ExtractTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	ext := NewMySQLExtractor()

	// Mock table list query
	tableRows := sqlmock.NewRows([]string{"table_name"}).
		AddRow("users").
		AddRow("orders")
	mock.ExpectQuery("SELECT table_name FROM information_schema.tables").
		WithArgs("testdb").
		WillReturnRows(tableRows)

	// Mock SHOW CREATE TABLE for users
	createTableUsers := "CREATE TABLE `users` (`id` int NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`))"
	mock.ExpectQuery("SHOW CREATE TABLE `testdb`.`users`").
		WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("users", createTableUsers))

	// Mock index query for users
	mock.ExpectQuery("SELECT index_name, GROUP_CONCAT.*FROM information_schema.statistics").
		WithArgs("testdb", "users").
		WillReturnRows(sqlmock.NewRows([]string{"index_name", "columns", "non_unique"}))

	// Mock foreign key query for users
	mock.ExpectQuery("SELECT.*kcu.constraint_name.*FROM information_schema.key_column_usage").
		WithArgs("testdb", "users").
		WillReturnRows(sqlmock.NewRows([]string{"constraint_name", "columns", "referenced_table_name", "ref_columns", "delete_rule", "update_rule"}))

	// Mock SHOW CREATE TABLE for orders
	createTableOrders := "CREATE TABLE `orders` (`id` int NOT NULL AUTO_INCREMENT, `user_id` int, PRIMARY KEY (`id`))"
	mock.ExpectQuery("SHOW CREATE TABLE `testdb`.`orders`").
		WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("orders", createTableOrders))

	// Mock index query for orders
	mock.ExpectQuery("SELECT index_name, GROUP_CONCAT.*FROM information_schema.statistics").
		WithArgs("testdb", "orders").
		WillReturnRows(sqlmock.NewRows([]string{"index_name", "columns", "non_unique"}))

	// Mock foreign key query for orders
	mock.ExpectQuery("SELECT.*kcu.constraint_name.*FROM information_schema.key_column_usage").
		WithArgs("testdb", "orders").
		WillReturnRows(sqlmock.NewRows([]string{"constraint_name", "columns", "referenced_table_name", "ref_columns", "delete_rule", "update_rule"}))

	tables, err := ext.ExtractTables(db, "testdb")
	if err != nil {
		t.Errorf("ExtractTables() error = %v", err)
	}

	if len(tables) != 2 {
		t.Errorf("ExtractTables() returned %d tables, want 2", len(tables))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestMySQLExtractor_ExtractTables_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	ext := NewMySQLExtractor()

	mock.ExpectQuery("SELECT table_name FROM information_schema.tables").
		WithArgs("testdb").
		WillReturnError(sqlmock.ErrCancelled)

	_, err = ext.ExtractTables(db, "testdb")
	if err == nil {
		t.Errorf("ExtractTables() expected error, got nil")
	}
}

func TestMySQLExtractor_ExtractViews(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	ext := NewMySQLExtractor()

	// Mock views query
	viewRows := sqlmock.NewRows([]string{"table_name", "view_definition"}).
		AddRow("user_stats", "SELECT * FROM users")
	mock.ExpectQuery("SELECT table_name, view_definition FROM information_schema.views").
		WithArgs("testdb").
		WillReturnRows(viewRows)

	// Mock SHOW CREATE VIEW
	createViewStmt := "CREATE VIEW `user_stats` AS SELECT * FROM users"
	mock.ExpectQuery("SHOW CREATE VIEW `testdb`.`user_stats`").
		WillReturnRows(sqlmock.NewRows([]string{"View", "Create View", "character_set_client", "collation_connection"}).
			AddRow("user_stats", createViewStmt, "utf8mb4", "utf8mb4_unicode_ci"))

	views, err := ext.ExtractViews(db, "testdb")
	if err != nil {
		t.Errorf("ExtractViews() error = %v", err)
	}

	if len(views) != 1 {
		t.Errorf("ExtractViews() returned %d views, want 1", len(views))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestMySQLExtractor_ExtractViews_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	ext := NewMySQLExtractor()

	mock.ExpectQuery("SELECT table_name, view_definition FROM information_schema.views").
		WithArgs("testdb").
		WillReturnError(sqlmock.ErrCancelled)

	_, err = ext.ExtractViews(db, "testdb")
	if err == nil {
		t.Errorf("ExtractViews() expected error, got nil")
	}
}

func TestMySQLExtractor_ExtractTables_WithIndexesAndFKs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	ext := NewMySQLExtractor()

	// Mock table list
	mock.ExpectQuery("SELECT table_name FROM information_schema.tables").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("users"))

	// Mock SHOW CREATE TABLE
	mock.ExpectQuery("SHOW CREATE TABLE `testdb`.`users`").
		WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("users", "CREATE TABLE `users` (`id` int PRIMARY KEY, `email` varchar(255))"))

	// Mock indexes
	mock.ExpectQuery("SELECT index_name.*FROM information_schema.statistics").
		WithArgs("testdb", "users").
		WillReturnRows(sqlmock.NewRows([]string{"index_name", "columns", "non_unique"}).
			AddRow("idx_email", "email", 1).
			AddRow("idx_unique", "email", 0))

	// Mock foreign keys
	mock.ExpectQuery("SELECT.*kcu.constraint_name.*FROM information_schema.key_column_usage").
		WithArgs("testdb", "users").
		WillReturnRows(sqlmock.NewRows([]string{"constraint_name", "columns", "referenced_table_name", "ref_columns", "delete_rule", "update_rule"}).
			AddRow("fk_dept", "dept_id", "departments", "id", "CASCADE", "NO ACTION"))

	tables, err := ext.ExtractTables(db, "testdb")
	if err != nil {
		t.Errorf("ExtractTables() error = %v", err)
	}

	if len(tables) != 1 {
		t.Fatalf("ExtractTables() returned %d tables, want 1", len(tables))
	}

	if len(tables[0].Indexes) != 2 {
		t.Errorf("table.Indexes = %d, want 2", len(tables[0].Indexes))
	}

	if len(tables[0].ForeignKeys) != 1 {
		t.Errorf("table.ForeignKeys = %d, want 1", len(tables[0].ForeignKeys))
	}

	// Check index uniqueness
	hasUniqueIndex := false
	for _, idx := range tables[0].Indexes {
		if idx.IsUnique {
			hasUniqueIndex = true
		}
	}
	if !hasUniqueIndex {
		t.Errorf("Expected at least one unique index")
	}

	// Check FK details
	fk := tables[0].ForeignKeys[0]
	if fk.OnDelete != "CASCADE" {
		t.Errorf("FK.OnDelete = %q, want %q", fk.OnDelete, "CASCADE")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestMySQLExtractor_ExtractTable_ShowCreateError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	ext := NewMySQLExtractor()

	// Mock table list
	mock.ExpectQuery("SELECT table_name FROM information_schema.tables").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("users"))

	// Mock SHOW CREATE TABLE failure
	mock.ExpectQuery("SHOW CREATE TABLE").
		WillReturnError(sqlmock.ErrCancelled)

	_, err = ext.ExtractTables(db, "testdb")
	if err == nil {
		t.Errorf("ExtractTables() expected error for SHOW CREATE TABLE failure")
	}
}

func TestMySQLExtractor_ExtractTable_IndexQueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	ext := NewMySQLExtractor()

	// Mock table list
	mock.ExpectQuery("SELECT table_name FROM information_schema.tables").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("users"))

	// Mock SHOW CREATE TABLE
	mock.ExpectQuery("SHOW CREATE TABLE").
		WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("users", "CREATE TABLE `users` (`id` int)"))

	// Mock index query failure
	mock.ExpectQuery("SELECT index_name.*FROM information_schema.statistics").
		WithArgs("testdb", "users").
		WillReturnError(sqlmock.ErrCancelled)

	_, err = ext.ExtractTables(db, "testdb")
	if err == nil {
		t.Errorf("ExtractTables() expected error for index query failure")
	}
}

func TestMySQLExtractor_ExtractTable_FKQueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	ext := NewMySQLExtractor()

	// Mock table list
	mock.ExpectQuery("SELECT table_name FROM information_schema.tables").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("users"))

	// Mock SHOW CREATE TABLE
	mock.ExpectQuery("SHOW CREATE TABLE").
		WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("users", "CREATE TABLE `users` (`id` int)"))

	// Mock indexes - empty
	mock.ExpectQuery("SELECT index_name.*FROM information_schema.statistics").
		WithArgs("testdb", "users").
		WillReturnRows(sqlmock.NewRows([]string{"index_name", "columns", "non_unique"}))

	// Mock FK query failure
	mock.ExpectQuery("SELECT.*kcu.constraint_name.*FROM information_schema.key_column_usage").
		WithArgs("testdb", "users").
		WillReturnError(sqlmock.ErrCancelled)

	_, err = ext.ExtractTables(db, "testdb")
	if err == nil {
		t.Errorf("ExtractTables() expected error for FK query failure")
	}
}

func TestMySQLExtractor_ExtractViews_ShowCreateViewError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	ext := NewMySQLExtractor()

	// Mock views query
	mock.ExpectQuery("SELECT table_name, view_definition FROM information_schema.views").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{"table_name", "view_definition"}).
			AddRow("user_stats", "SELECT * FROM users"))

	// Mock SHOW CREATE VIEW failure
	mock.ExpectQuery("SHOW CREATE VIEW").
		WillReturnError(sqlmock.ErrCancelled)

	_, err = ext.ExtractViews(db, "testdb")
	if err == nil {
		t.Errorf("ExtractViews() expected error for SHOW CREATE VIEW failure")
	}
}
