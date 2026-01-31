package migrator

import (
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/DGarbs51/lcmigrate/internal/config"
	"github.com/DGarbs51/lcmigrate/internal/data"
	"github.com/DGarbs51/lcmigrate/internal/schema"
)

// MockExtractor implements schema.Extractor for testing
type MockExtractor struct {
	Tables    []schema.TableSchema
	Views     []schema.ViewDef
	Sequences []schema.SequenceDef
	Err       error
}

func (m *MockExtractor) ExtractTables(db *sql.DB, database string) ([]schema.TableSchema, error) {
	return m.Tables, m.Err
}

func (m *MockExtractor) ExtractViews(db *sql.DB, database string) ([]schema.ViewDef, error) {
	return m.Views, m.Err
}

func (m *MockExtractor) ExtractSequences(db *sql.DB, database string) ([]schema.SequenceDef, error) {
	return m.Sequences, m.Err
}

// MockApplier implements schema.Applier for testing
type MockApplier struct {
	TablesCreated   int
	IndexesCreated  int
	FKsCreated      int
	ViewsCreated    int
	SequencesSet    int
	Err             error
}

func (m *MockApplier) CreateTable(db *sql.DB, table schema.TableSchema) error {
	m.TablesCreated++
	return m.Err
}

func (m *MockApplier) CreateIndex(db *sql.DB, index schema.IndexDef) error {
	m.IndexesCreated++
	return m.Err
}

func (m *MockApplier) CreateForeignKey(db *sql.DB, fk schema.ForeignKeyDef) error {
	m.FKsCreated++
	return m.Err
}

func (m *MockApplier) CreateView(db *sql.DB, view schema.ViewDef) error {
	m.ViewsCreated++
	return m.Err
}

func (m *MockApplier) CreateSequence(db *sql.DB, seq schema.SequenceDef) error {
	return m.Err
}

func (m *MockApplier) SetSequenceValue(db *sql.DB, seq schema.SequenceDef) error {
	m.SequencesSet++
	return m.Err
}

// MockTransferer implements data.Transferer for testing
type MockTransferer struct {
	DisableFKCalls int
	EnableFKCalls  int
	TransferCalls  int
	RowsCopied     int64
	Err            error
}

func (m *MockTransferer) DisableForeignKeyChecks(dest *sql.DB) error {
	m.DisableFKCalls++
	return m.Err
}

func (m *MockTransferer) EnableForeignKeyChecks(dest *sql.DB) error {
	m.EnableFKCalls++
	return m.Err
}

func (m *MockTransferer) TransferTable(source, dest *sql.DB, table schema.TableSchema, batchSize int, dryRun bool, progressFn func(rows int64)) (*data.TransferStats, error) {
	m.TransferCalls++
	return &data.TransferStats{
		TableName:  table.Name,
		RowsCopied: m.RowsCopied,
		Duration:   100 * time.Millisecond,
	}, m.Err
}

func (m *MockTransferer) EstimateRows(db *sql.DB, table string) (int64, error) {
	return m.RowsCopied, m.Err
}

func TestMigrator_Constants(t *testing.T) {
	if DefaultBatchSize != 10000 {
		t.Errorf("DefaultBatchSize = %d, want 10000", DefaultBatchSize)
	}
	if TotalStages != 6 {
		t.Errorf("TotalStages = %d, want 6", TotalStages)
	}
}

func TestMigrator_MigrateSchema_DryRun(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	extractor := &MockExtractor{
		Tables: []schema.TableSchema{
			{Name: "users"},
			{Name: "orders"},
		},
	}
	applier := &MockApplier{}

	m := &Migrator{
		config: config.MigrationConfig{
			Source:      config.DatabaseConfig{Database: "testdb"},
			Destination: config.DatabaseConfig{Database: "testdb_dest"},
			DryRun:      true,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		extractor:  extractor,
		applier:    applier,
	}

	err = m.migrateSchema()
	if err != nil {
		t.Errorf("migrateSchema() error = %v", err)
	}

	// In dry run, tables should be extracted but not created
	if len(m.tables) != 2 {
		t.Errorf("m.tables = %d, want 2", len(m.tables))
	}
	if applier.TablesCreated != 0 {
		t.Errorf("TablesCreated in dry run = %d, want 0", applier.TablesCreated)
	}
}

func TestMigrator_MigrateSchema_CreateTables(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	extractor := &MockExtractor{
		Tables: []schema.TableSchema{
			{Name: "users"},
			{Name: "orders"},
		},
	}
	applier := &MockApplier{}

	m := &Migrator{
		config: config.MigrationConfig{
			Source:      config.DatabaseConfig{Database: "testdb"},
			Destination: config.DatabaseConfig{Database: "testdb_dest"},
			DryRun:      false,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		extractor:  extractor,
		applier:    applier,
	}

	err = m.migrateSchema()
	if err != nil {
		t.Errorf("migrateSchema() error = %v", err)
	}

	if applier.TablesCreated != 2 {
		t.Errorf("TablesCreated = %d, want 2", applier.TablesCreated)
	}
}

func TestMigrator_CreateIndexesAndConstraints(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	applier := &MockApplier{}

	m := &Migrator{
		config: config.MigrationConfig{
			DryRun: false,
		},
		destConn: destDB,
		applier:  applier,
		tables: []schema.TableSchema{
			{
				Name: "users",
				Indexes: []schema.IndexDef{
					{Name: "idx_email"},
				},
				ForeignKeys: []schema.ForeignKeyDef{
					{Name: "fk_user"},
				},
			},
			{
				Name: "orders",
				Indexes: []schema.IndexDef{
					{Name: "idx_date"},
					{Name: "idx_user"},
				},
				ForeignKeys: []schema.ForeignKeyDef{},
			},
		},
	}

	err = m.createIndexesAndConstraints()
	if err != nil {
		t.Errorf("createIndexesAndConstraints() error = %v", err)
	}

	if applier.IndexesCreated != 3 {
		t.Errorf("IndexesCreated = %d, want 3", applier.IndexesCreated)
	}
	if applier.FKsCreated != 1 {
		t.Errorf("FKsCreated = %d, want 1", applier.FKsCreated)
	}
}

func TestMigrator_CreateIndexesAndConstraints_DryRun(t *testing.T) {
	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	applier := &MockApplier{}

	m := &Migrator{
		config: config.MigrationConfig{
			DryRun: true,
		},
		destConn: destDB,
		applier:  applier,
		tables: []schema.TableSchema{
			{
				Name:    "users",
				Indexes: []schema.IndexDef{{Name: "idx_email"}},
			},
		},
	}

	err = m.createIndexesAndConstraints()
	if err != nil {
		t.Errorf("createIndexesAndConstraints() error = %v", err)
	}

	// Dry run should not create anything
	if applier.IndexesCreated != 0 {
		t.Errorf("IndexesCreated in dry run = %d, want 0", applier.IndexesCreated)
	}
}

func TestMigrator_CreateViews(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	extractor := &MockExtractor{
		Views: []schema.ViewDef{
			{Name: "user_stats"},
			{Name: "order_summary"},
		},
	}
	applier := &MockApplier{}

	m := &Migrator{
		config: config.MigrationConfig{
			Source: config.DatabaseConfig{Database: "testdb"},
			DryRun: false,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		extractor:  extractor,
		applier:    applier,
	}

	err = m.createViews()
	if err != nil {
		t.Errorf("createViews() error = %v", err)
	}

	if applier.ViewsCreated != 2 {
		t.Errorf("ViewsCreated = %d, want 2", applier.ViewsCreated)
	}
}

func TestMigrator_CreateViews_NoViews(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	extractor := &MockExtractor{
		Views: []schema.ViewDef{},
	}
	applier := &MockApplier{}

	m := &Migrator{
		config: config.MigrationConfig{
			Source: config.DatabaseConfig{Database: "testdb"},
			DryRun: false,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		extractor:  extractor,
		applier:    applier,
	}

	err = m.createViews()
	if err != nil {
		t.Errorf("createViews() error = %v", err)
	}

	// No views to create
	if applier.ViewsCreated != 0 {
		t.Errorf("ViewsCreated = %d, want 0", applier.ViewsCreated)
	}
}

func TestMigrator_MigrateSequences_MySQL(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	m := &Migrator{
		config: config.MigrationConfig{
			Source: config.DatabaseConfig{Engine: "mysql"},
			DryRun: false,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
	}

	// MySQL should skip sequences
	err = m.migrateSequences()
	if err != nil {
		t.Errorf("migrateSequences() error = %v", err)
	}
}

func TestMigrator_MigrateSequences_Postgres(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	extractor := &MockExtractor{
		Sequences: []schema.SequenceDef{
			{Name: "users_id_seq", CurrentVal: 100},
			{Name: "orders_id_seq", CurrentVal: 50},
		},
	}
	applier := &MockApplier{}

	m := &Migrator{
		config: config.MigrationConfig{
			Source: config.DatabaseConfig{Engine: "pgsql", Database: "testdb"},
			DryRun: false,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		extractor:  extractor,
		applier:    applier,
	}

	err = m.migrateSequences()
	if err != nil {
		t.Errorf("migrateSequences() error = %v", err)
	}

	if applier.SequencesSet != 2 {
		t.Errorf("SequencesSet = %d, want 2", applier.SequencesSet)
	}
}

func TestMigrator_MigrateSequences_NoSequences(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	extractor := &MockExtractor{
		Sequences: []schema.SequenceDef{},
	}
	applier := &MockApplier{}

	m := &Migrator{
		config: config.MigrationConfig{
			Source: config.DatabaseConfig{Engine: "pgsql", Database: "testdb"},
			DryRun: false,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		extractor:  extractor,
		applier:    applier,
	}

	err = m.migrateSequences()
	if err != nil {
		t.Errorf("migrateSequences() error = %v", err)
	}
}

func TestMigrator_MigrateSequences_DryRun(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	extractor := &MockExtractor{
		Sequences: []schema.SequenceDef{
			{Name: "users_id_seq", CurrentVal: 100},
		},
	}
	applier := &MockApplier{}

	m := &Migrator{
		config: config.MigrationConfig{
			Source: config.DatabaseConfig{Engine: "pgsql", Database: "testdb"},
			DryRun: true,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		extractor:  extractor,
		applier:    applier,
	}

	err = m.migrateSequences()
	if err != nil {
		t.Errorf("migrateSequences() error = %v", err)
	}

	// Dry run should not set sequence values
	if applier.SequencesSet != 0 {
		t.Errorf("SequencesSet in dry run = %d, want 0", applier.SequencesSet)
	}
}

func TestMigrator_CreateViews_DryRun(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	extractor := &MockExtractor{
		Views: []schema.ViewDef{
			{Name: "user_stats"},
		},
	}
	applier := &MockApplier{}

	m := &Migrator{
		config: config.MigrationConfig{
			Source: config.DatabaseConfig{Database: "testdb"},
			DryRun: true,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		extractor:  extractor,
		applier:    applier,
	}

	err = m.createViews()
	if err != nil {
		t.Errorf("createViews() error = %v", err)
	}

	// Dry run should not create views
	if applier.ViewsCreated != 0 {
		t.Errorf("ViewsCreated in dry run = %d, want 0", applier.ViewsCreated)
	}
}

func TestMigrator_MigrateData_DryRun(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	transferer := &MockTransferer{
		RowsCopied: 100,
	}

	m := &Migrator{
		config: config.MigrationConfig{
			DryRun: true,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		transferer: transferer,
		tables: []schema.TableSchema{
			{Name: "users"},
			{Name: "orders"},
		},
	}

	err = m.migrateData()
	if err != nil {
		t.Errorf("migrateData() error = %v", err)
	}

	// Dry run should not disable FK checks or transfer data
	if transferer.DisableFKCalls != 0 {
		t.Errorf("DisableFKCalls in dry run = %d, want 0", transferer.DisableFKCalls)
	}
	if transferer.TransferCalls != 0 {
		t.Errorf("TransferCalls in dry run = %d, want 0", transferer.TransferCalls)
	}
	// But totalRows should be estimated
	if m.totalRows != 200 { // 2 tables * 100 rows each
		t.Errorf("totalRows in dry run = %d, want 200", m.totalRows)
	}
}

func TestMigrator_MigrateData_Transfer(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	transferer := &MockTransferer{
		RowsCopied: 50,
	}

	m := &Migrator{
		config: config.MigrationConfig{
			DryRun: false,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		transferer: transferer,
		tables: []schema.TableSchema{
			{Name: "users"},
			{Name: "orders"},
		},
	}

	err = m.migrateData()
	if err != nil {
		t.Errorf("migrateData() error = %v", err)
	}

	if transferer.DisableFKCalls != 1 {
		t.Errorf("DisableFKCalls = %d, want 1", transferer.DisableFKCalls)
	}
	if transferer.TransferCalls != 2 {
		t.Errorf("TransferCalls = %d, want 2", transferer.TransferCalls)
	}
	if m.totalRows != 100 { // 2 tables * 50 rows each
		t.Errorf("totalRows = %d, want 100", m.totalRows)
	}
}

func TestMigrator_Finalize_DryRun(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	transferer := &MockTransferer{}

	m := &Migrator{
		config: config.MigrationConfig{
			DryRun: true,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		transferer: transferer,
		tables: []schema.TableSchema{
			{Name: "users"},
		},
	}

	err = m.finalize()
	if err != nil {
		t.Errorf("finalize() error = %v", err)
	}

	// Dry run should not enable FK checks
	if transferer.EnableFKCalls != 0 {
		t.Errorf("EnableFKCalls in dry run = %d, want 0", transferer.EnableFKCalls)
	}
}

func TestMigrator_Finalize_EnableFKAndVerify(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	transferer := &MockTransferer{
		RowsCopied: 100, // Same count for source and dest
	}

	m := &Migrator{
		config: config.MigrationConfig{
			DryRun: false,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		transferer: transferer,
		tables: []schema.TableSchema{
			{Name: "users"},
		},
	}

	err = m.finalize()
	if err != nil {
		t.Errorf("finalize() error = %v", err)
	}

	if transferer.EnableFKCalls != 1 {
		t.Errorf("EnableFKCalls = %d, want 1", transferer.EnableFKCalls)
	}
}

func TestMigrator_MigrateSchema_Error(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	extractor := &MockExtractor{
		Err: sqlmock.ErrCancelled,
	}
	applier := &MockApplier{}

	m := &Migrator{
		config: config.MigrationConfig{
			Source: config.DatabaseConfig{Database: "testdb"},
			DryRun: false,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		extractor:  extractor,
		applier:    applier,
	}

	err = m.migrateSchema()
	if err == nil {
		t.Errorf("migrateSchema() expected error, got nil")
	}
}

func TestMigrator_CreateViews_ExtractError(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	extractor := &MockExtractor{
		Err: sqlmock.ErrCancelled,
	}
	applier := &MockApplier{}

	m := &Migrator{
		config: config.MigrationConfig{
			Source: config.DatabaseConfig{Database: "testdb"},
			DryRun: false,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		extractor:  extractor,
		applier:    applier,
	}

	err = m.createViews()
	if err == nil {
		t.Errorf("createViews() expected error, got nil")
	}
}

func TestMigrator_MigrateSequences_ExtractError(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	extractor := &MockExtractor{
		Err: sqlmock.ErrCancelled,
	}
	applier := &MockApplier{}

	m := &Migrator{
		config: config.MigrationConfig{
			Source: config.DatabaseConfig{Engine: "pgsql", Database: "testdb"},
			DryRun: false,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		extractor:  extractor,
		applier:    applier,
	}

	err = m.migrateSequences()
	if err == nil {
		t.Errorf("migrateSequences() expected error, got nil")
	}
}

func TestMigrator_CreateTable_Error(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	extractor := &MockExtractor{
		Tables: []schema.TableSchema{
			{Name: "users"},
		},
	}
	applier := &MockApplier{
		Err: sqlmock.ErrCancelled,
	}

	m := &Migrator{
		config: config.MigrationConfig{
			Source:      config.DatabaseConfig{Database: "testdb"},
			Destination: config.DatabaseConfig{Database: "testdb_dest"},
			DryRun:      false,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		extractor:  extractor,
		applier:    applier,
	}

	err = m.migrateSchema()
	if err == nil {
		t.Errorf("migrateSchema() with CreateTable error expected error, got nil")
	}
}

func TestMigrator_CreateIndex_Error(t *testing.T) {
	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	applier := &MockApplier{
		Err: sqlmock.ErrCancelled,
	}

	m := &Migrator{
		config: config.MigrationConfig{
			DryRun: false,
		},
		destConn: destDB,
		applier:  applier,
		tables: []schema.TableSchema{
			{
				Name:    "users",
				Indexes: []schema.IndexDef{{Name: "idx_email"}},
			},
		},
	}

	err = m.createIndexesAndConstraints()
	if err == nil {
		t.Errorf("createIndexesAndConstraints() with CreateIndex error expected error, got nil")
	}
}

func TestMigrator_CreateForeignKey_Error(t *testing.T) {
	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	// Applier that succeeds on indexes but fails on FKs
	applier := &MockApplier{}
	applier.Err = nil

	m := &Migrator{
		config: config.MigrationConfig{
			DryRun: false,
		},
		destConn: destDB,
		applier:  applier,
		tables: []schema.TableSchema{
			{
				Name:        "users",
				Indexes:     []schema.IndexDef{},
				ForeignKeys: []schema.ForeignKeyDef{{Name: "fk_user"}},
			},
		},
	}

	// Set error after init
	applier.Err = sqlmock.ErrCancelled

	err = m.createIndexesAndConstraints()
	if err == nil {
		t.Errorf("createIndexesAndConstraints() with CreateForeignKey error expected error, got nil")
	}
}

func TestMigrator_CreateView_Error(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	extractor := &MockExtractor{
		Views: []schema.ViewDef{
			{Name: "user_stats"},
		},
	}
	applier := &MockApplier{
		Err: sqlmock.ErrCancelled,
	}

	m := &Migrator{
		config: config.MigrationConfig{
			Source: config.DatabaseConfig{Database: "testdb"},
			DryRun: false,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		extractor:  extractor,
		applier:    applier,
	}

	err = m.createViews()
	if err == nil {
		t.Errorf("createViews() with CreateView error expected error, got nil")
	}
}

func TestMigrator_SetSequenceValue_Error(t *testing.T) {
	sourceDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create source mock: %v", err)
	}
	defer sourceDB.Close()

	destDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create dest mock: %v", err)
	}
	defer destDB.Close()

	extractor := &MockExtractor{
		Sequences: []schema.SequenceDef{
			{Name: "users_id_seq", CurrentVal: 100},
		},
	}
	applier := &MockApplier{
		Err: sqlmock.ErrCancelled,
	}

	m := &Migrator{
		config: config.MigrationConfig{
			Source: config.DatabaseConfig{Engine: "pgsql", Database: "testdb"},
			DryRun: false,
		},
		sourceConn: sourceDB,
		destConn:   destDB,
		extractor:  extractor,
		applier:    applier,
	}

	err = m.migrateSequences()
	if err == nil {
		t.Errorf("migrateSequences() with SetSequenceValue error expected error, got nil")
	}
}

func TestMigrator_Struct_Fields(t *testing.T) {
	m := &Migrator{
		config: config.MigrationConfig{
			DryRun: true,
		},
		tables: []schema.TableSchema{
			{Name: "users"},
		},
		views: []schema.ViewDef{
			{Name: "user_stats"},
		},
		sequences: []schema.SequenceDef{
			{Name: "users_id_seq"},
		},
		totalRows: 1000,
	}

	if !m.config.DryRun {
		t.Errorf("Migrator.config.DryRun = %v, want true", m.config.DryRun)
	}
	if len(m.tables) != 1 {
		t.Errorf("len(Migrator.tables) = %d, want 1", len(m.tables))
	}
	if len(m.views) != 1 {
		t.Errorf("len(Migrator.views) = %d, want 1", len(m.views))
	}
	if len(m.sequences) != 1 {
		t.Errorf("len(Migrator.sequences) = %d, want 1", len(m.sequences))
	}
	if m.totalRows != 1000 {
		t.Errorf("Migrator.totalRows = %d, want 1000", m.totalRows)
	}
}
