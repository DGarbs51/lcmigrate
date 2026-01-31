package data

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/DGarbs51/lcmigrate/internal/schema"
)

// MySQLTransferer handles data transfer for MySQL databases
type MySQLTransferer struct{}

// DisableForeignKeyChecks disables FK checks for the session
func (t *MySQLTransferer) DisableForeignKeyChecks(dest *sql.DB) error {
	_, err := dest.Exec("SET FOREIGN_KEY_CHECKS = 0")
	return err
}

// EnableForeignKeyChecks enables FK checks for the session
func (t *MySQLTransferer) EnableForeignKeyChecks(dest *sql.DB) error {
	_, err := dest.Exec("SET FOREIGN_KEY_CHECKS = 1")
	return err
}

// EstimateRows returns the estimated row count for a table
func (t *MySQLTransferer) EstimateRows(db *sql.DB, table string) (int64, error) {
	var count int64
	err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)).Scan(&count)
	return count, err
}

// TransferTable copies all data from source to destination
func (t *MySQLTransferer) TransferTable(source, dest *sql.DB, table schema.TableSchema, batchSize int, dryRun bool, progressFn func(rows int64)) (*TransferStats, error) {
	startTime := time.Now()
	stats := &TransferStats{
		TableName: table.Name,
	}

	// Get column names
	columns, err := t.getColumns(source, table.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	if len(columns) == 0 {
		return stats, nil
	}

	// Get total row count for progress
	totalRows, err := t.EstimateRows(source, table.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to estimate rows: %w", err)
	}

	if dryRun {
		stats.RowsCopied = totalRows
		stats.Duration = time.Since(startTime)
		return stats, nil
	}

	// Build column list
	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = "`" + col + "`"
	}
	colList := strings.Join(quotedCols, ", ")

	// Transfer in batches
	offset := int64(0)
	for {
		// Read batch from source
		rows, err := source.Query(fmt.Sprintf(
			"SELECT %s FROM `%s` LIMIT %d OFFSET %d",
			colList, table.Name, batchSize, offset,
		))
		if err != nil {
			return nil, fmt.Errorf("failed to read from source: %w", err)
		}

		// Collect batch data
		var batch [][]interface{}
		for rows.Next() {
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to scan row: %w", err)
			}
			batch = append(batch, values)
		}
		rows.Close()

		if len(batch) == 0 {
			break
		}

		// Insert batch into destination
		if err := t.insertBatch(dest, table.Name, columns, batch); err != nil {
			return nil, fmt.Errorf("failed to insert batch: %w", err)
		}

		stats.RowsCopied += int64(len(batch))
		offset += int64(len(batch))

		if progressFn != nil {
			progressFn(stats.RowsCopied)
		}

		// Check if we've processed all rows
		if int64(len(batch)) < int64(batchSize) {
			break
		}
	}

	stats.Duration = time.Since(startTime)
	return stats, nil
}

// getColumns returns the column names for a table
func (t *MySQLTransferer) getColumns(db *sql.DB, table string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT 0", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	return columns, nil
}

// insertBatch inserts a batch of rows into the destination table
func (t *MySQLTransferer) insertBatch(dest *sql.DB, table string, columns []string, batch [][]interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	// Build INSERT statement with multiple value sets
	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = "`" + col + "`"
	}

	// Build placeholders for each row
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	rowPlaceholder := "(" + strings.Join(placeholders, ", ") + ")"

	rowPlaceholders := make([]string, len(batch))
	for i := range rowPlaceholders {
		rowPlaceholders[i] = rowPlaceholder
	}

	query := fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES %s",
		table,
		strings.Join(quotedCols, ", "),
		strings.Join(rowPlaceholders, ", "),
	)

	// Flatten values
	var args []interface{}
	for _, row := range batch {
		args = append(args, row...)
	}

	_, err := dest.Exec(query, args...)
	return err
}
