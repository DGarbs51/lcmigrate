package data

import (
	"database/sql"
	"time"

	"github.com/DGarbs51/lcmigrate/internal/schema"
)

// TransferStats contains statistics for a single table transfer
type TransferStats struct {
	TableName  string
	RowsCopied int64
	Duration   time.Duration
}

// Transferer defines the interface for transferring data between databases
type Transferer interface {
	// DisableForeignKeyChecks disables foreign key checks on the destination
	DisableForeignKeyChecks(dest *sql.DB) error

	// EnableForeignKeyChecks enables foreign key checks on the destination
	EnableForeignKeyChecks(dest *sql.DB) error

	// TransferTable copies all data from a table in the source to the destination
	TransferTable(source, dest *sql.DB, table schema.TableSchema, batchSize int, dryRun bool, progressFn func(rows int64)) (*TransferStats, error)

	// EstimateRows returns the estimated row count for a table
	EstimateRows(db *sql.DB, table string) (int64, error)
}

// NewTransferer creates a data transferer for the given engine
func NewTransferer(engine string) Transferer {
	switch engine {
	case "mysql":
		return &MySQLTransferer{}
	case "pgsql":
		return &PostgresTransferer{}
	default:
		return nil
	}
}
