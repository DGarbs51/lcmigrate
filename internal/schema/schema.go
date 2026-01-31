package schema

import (
	"database/sql"
	"fmt"

	"github.com/DGarbs51/lcmigrate/internal/dialect"
)

// TableSchema represents the schema of a database table
type TableSchema struct {
	Name        string
	CreateStmt  string // Full CREATE TABLE statement
	Columns     []ColumnDef
	PrimaryKey  []string
	Indexes     []IndexDef
	ForeignKeys []ForeignKeyDef
}

// ColumnDef represents a column definition
type ColumnDef struct {
	Name         string
	DataType     string
	IsNullable   bool
	DefaultValue sql.NullString
	Extra        string // AUTO_INCREMENT, etc.
}

// IndexDef represents an index definition
type IndexDef struct {
	Name       string
	Columns    []string
	IsUnique   bool
	IsPrimary  bool
	CreateStmt string // Full CREATE INDEX statement
}

// ForeignKeyDef represents a foreign key constraint
type ForeignKeyDef struct {
	Name           string
	Columns        []string
	RefTable       string
	RefColumns     []string
	OnDelete       string
	OnUpdate       string
	ConstraintStmt string // Full ALTER TABLE ADD CONSTRAINT statement
}

// ViewDef represents a view definition
type ViewDef struct {
	Name         string
	CreateStmt   string
	Dependencies []string // Other views this view depends on
}

// SequenceDef represents a sequence (PostgreSQL)
type SequenceDef struct {
	Name       string
	CreateStmt string
	CurrentVal int64
	OwnedBy    string // table.column that owns this sequence
}

// Extractor defines the interface for extracting schema information
type Extractor interface {
	ExtractTables(db *sql.DB, database string) ([]TableSchema, error)
	ExtractViews(db *sql.DB, database string) ([]ViewDef, error)
	ExtractSequences(db *sql.DB, database string) ([]SequenceDef, error)
}

// Applier defines the interface for applying schema to a database
type Applier interface {
	CreateTable(db *sql.DB, table TableSchema) error
	CreateIndex(db *sql.DB, index IndexDef) error
	CreateForeignKey(db *sql.DB, fk ForeignKeyDef) error
	CreateView(db *sql.DB, view ViewDef) error
	CreateSequence(db *sql.DB, seq SequenceDef) error
	SetSequenceValue(db *sql.DB, seq SequenceDef) error
}

// NewExtractor creates a schema extractor for the given engine
func NewExtractor(engine string) Extractor {
	switch engine {
	case "mysql":
		return NewMySQLExtractor()
	case "pgsql":
		return NewPostgresExtractor()
	default:
		return nil
	}
}

// NewApplier creates a schema applier for the given engine
func NewApplier(engine string) Applier {
	switch engine {
	case "mysql":
		return NewMySQLApplier()
	case "pgsql":
		return NewPostgresApplier()
	default:
		return nil
	}
}

// BaseApplier contains shared schema application logic
type BaseApplier struct {
	Dialect dialect.Dialect
}

// CreateIndex creates an index on a table
func (a *BaseApplier) CreateIndex(db *sql.DB, index IndexDef) error {
	_, err := db.Exec(index.CreateStmt)
	if err != nil {
		return fmt.Errorf("failed to create index %s: %w", index.Name, err)
	}
	return nil
}

// CreateForeignKey adds a foreign key constraint to a table
func (a *BaseApplier) CreateForeignKey(db *sql.DB, fk ForeignKeyDef) error {
	_, err := db.Exec(fk.ConstraintStmt)
	if err != nil {
		return fmt.Errorf("failed to create foreign key %s: %w", fk.Name, err)
	}
	return nil
}

// CreateView creates a view in the database
func (a *BaseApplier) CreateView(db *sql.DB, view ViewDef) error {
	_, err := db.Exec(view.CreateStmt)
	if err != nil {
		return fmt.Errorf("failed to create view %s: %w", view.Name, err)
	}
	return nil
}
