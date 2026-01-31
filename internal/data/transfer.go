package data

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/DGarbs51/lcmigrate/internal/dialect"
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
		return NewMySQLTransferer()
	case "pgsql":
		return NewPostgresTransferer()
	default:
		return nil
	}
}

// BaseTransferer contains shared transfer logic that works with any dialect
type BaseTransferer struct {
	Dialect dialect.Dialect
}

// DisableForeignKeyChecks executes the dialect-specific FK disable command
func (t *BaseTransferer) DisableForeignKeyChecks(dest *sql.DB) error {
	_, err := dest.Exec(t.Dialect.DisableFKChecksSQL())
	return err
}

// EnableForeignKeyChecks executes the dialect-specific FK enable command
func (t *BaseTransferer) EnableForeignKeyChecks(dest *sql.DB) error {
	_, err := dest.Exec(t.Dialect.EnableFKChecksSQL())
	return err
}

// EstimateRows counts rows in a table
func (t *BaseTransferer) EstimateRows(db *sql.DB, table string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", t.Dialect.QuoteIdentifier(table))
	err := db.QueryRow(query).Scan(&count)
	return count, err
}

// GetColumns returns column names for a table
func (t *BaseTransferer) GetColumns(db *sql.DB, table string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 0", t.Dialect.QuoteIdentifier(table)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return rows.Columns()
}

// TransferTable copies all data from source to destination
func (t *BaseTransferer) TransferTable(source, dest *sql.DB, table schema.TableSchema, batchSize int, dryRun bool, progressFn func(rows int64)) (*TransferStats, error) {
	startTime := time.Now()
	stats := &TransferStats{
		TableName: table.Name,
	}

	columns, err := t.GetColumns(source, table.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	if len(columns) == 0 {
		return stats, nil
	}

	totalRows, err := t.EstimateRows(source, table.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to estimate rows: %w", err)
	}

	if dryRun {
		stats.RowsCopied = totalRows
		stats.Duration = time.Since(startTime)
		return stats, nil
	}

	// Build quoted column list
	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = t.Dialect.QuoteIdentifier(col)
	}
	colList := strings.Join(quotedCols, ", ")
	quotedTable := t.Dialect.QuoteIdentifier(table.Name)

	// Transfer in batches
	offset := int64(0)
	for {
		query := fmt.Sprintf("SELECT %s FROM %s LIMIT %d OFFSET %d",
			colList, quotedTable, batchSize, offset)
		rows, err := source.Query(query)
		if err != nil {
			return nil, fmt.Errorf("failed to read from source: %w", err)
		}

		batch, err := t.collectBatch(rows, len(columns))
		rows.Close()
		if err != nil {
			return nil, err
		}

		if len(batch) == 0 {
			break
		}

		if err := t.InsertBatch(dest, table.Name, columns, batch); err != nil {
			return nil, fmt.Errorf("failed to insert batch: %w", err)
		}

		stats.RowsCopied += int64(len(batch))
		offset += int64(len(batch))

		if progressFn != nil {
			progressFn(stats.RowsCopied)
		}

		if int64(len(batch)) < int64(batchSize) {
			break
		}
	}

	stats.Duration = time.Since(startTime)
	return stats, nil
}

func (t *BaseTransferer) collectBatch(rows *sql.Rows, numCols int) ([][]interface{}, error) {
	var batch [][]interface{}
	for rows.Next() {
		values := make([]interface{}, numCols)
		valuePtrs := make([]interface{}, numCols)
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		batch = append(batch, values)
	}
	return batch, nil
}

// InsertBatch inserts a batch of rows into the destination table
func (t *BaseTransferer) InsertBatch(dest *sql.DB, table string, columns []string, batch [][]interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = t.Dialect.QuoteIdentifier(col)
	}

	// Build placeholders using dialect
	var allPlaceholders []string
	argIndex := 1
	for range batch {
		rowPlaceholders := make([]string, len(columns))
		for i := range rowPlaceholders {
			rowPlaceholders[i] = t.Dialect.Placeholder(argIndex)
			if t.Dialect.PlaceholderStyle() == dialect.PlaceholderPositional {
				argIndex++
			}
		}
		allPlaceholders = append(allPlaceholders, "("+strings.Join(rowPlaceholders, ", ")+")")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		t.Dialect.QuoteIdentifier(table),
		strings.Join(quotedCols, ", "),
		strings.Join(allPlaceholders, ", "))

	var args []interface{}
	for _, row := range batch {
		args = append(args, row...)
	}

	_, err := dest.Exec(query, args...)
	return err
}
