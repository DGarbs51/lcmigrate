package db

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/fatih/color"
)

func AnalyzeMySQL(db *sql.DB, database string) error {
	bold := color.New(color.Bold).SprintFunc()
	cyan := color.New(color.FgCyan).SprintFunc()
	green := color.New(color.FgGreen).SprintFunc()
	yellow := color.New(color.FgYellow).SprintFunc()
	dim := color.New(color.Faint).SprintFunc()

	// Server info
	fmt.Println()
	fmt.Printf("  %s\n", bold("🖥️  Server Information"))
	fmt.Printf("  %s\n\n", dim("─────────────────────────────"))

	var version string
	if err := db.QueryRow(`SELECT VERSION()`).Scan(&version); err != nil {
		return fmt.Errorf("failed to get version: %w", err)
	}
	fmt.Printf("  %-22s %s\n", cyan("Version:"), green(version))

	serverVars, err := db.Query(`SHOW VARIABLES WHERE Variable_name IN ('version_comment', 'max_connections', 'wait_timeout', 'character_set_server', 'collation_server', 'innodb_buffer_pool_size')`)
	if err != nil {
		return fmt.Errorf("failed to get server variables: %w", err)
	}
	defer serverVars.Close()

	for serverVars.Next() {
		var name, value string
		if err := serverVars.Scan(&name, &value); err != nil {
			return err
		}
		displayName := name
		switch name {
		case "version_comment":
			displayName = "Server Type"
		case "max_connections":
			displayName = "Max Connections"
		case "wait_timeout":
			displayName = "Wait Timeout"
			value = value + "s"
		case "character_set_server":
			displayName = "Character Set"
		case "collation_server":
			displayName = "Collation"
		case "innodb_buffer_pool_size":
			displayName = "InnoDB Buffer Pool"
			if size, err := strconv.ParseInt(value, 10, 64); err == nil {
				value = formatBytes(float64(size))
			}
		}
		fmt.Printf("  %-22s %s\n", cyan(displayName+":"), value)
	}

	// Uptime
	var uptime int64
	if err := db.QueryRow(`SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Uptime'`).Scan(&uptime); err == nil {
		fmt.Printf("  %-22s %s\n", cyan("Uptime:"), formatDuration(uptime))
	}

	// Table count
	var tableCount int
	err = db.QueryRow(`
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = ? AND table_type = 'BASE TABLE'
	`, database).Scan(&tableCount)
	if err != nil {
		return fmt.Errorf("failed to get table count: %w", err)
	}

	// Database size
	var dbSize sql.NullFloat64
	err = db.QueryRow(`
		SELECT SUM(data_length + index_length)
		FROM information_schema.tables
		WHERE table_schema = ?
	`, database).Scan(&dbSize)
	if err != nil {
		return fmt.Errorf("failed to get database size: %w", err)
	}

	// Summary box
	fmt.Println()
	fmt.Printf("  %s\n", bold("📊 Database Summary"))
	fmt.Printf("  %s\n\n", dim("─────────────────────────────"))
	fmt.Printf("  %s  %s\n", cyan("Tables:"), green(strconv.Itoa(tableCount)))
	if dbSize.Valid {
		fmt.Printf("  %s    %s\n", cyan("Size:"), green(formatBytes(dbSize.Float64)))
	} else {
		fmt.Printf("  %s    %s\n", cyan("Size:"), "0 bytes")
	}

	// Table details
	fmt.Println()
	fmt.Printf("  %s\n\n", bold("📋 Table Details"))

	rows, err := db.Query(`
		SELECT
			table_name,
			engine,
			table_rows,
			data_length + index_length as total_size
		FROM information_schema.tables
		WHERE table_schema = ? AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`, database)
	if err != nil {
		return fmt.Errorf("failed to get table details: %w", err)
	}
	defer rows.Close()

	fmt.Printf("  %-35s %-12s %12s %12s\n", bold("TABLE"), bold("ENGINE"), bold("EST. ROWS"), bold("SIZE"))
	fmt.Printf("  %s\n", dim(strings.Repeat("─", 75)))

	var totalRows int64
	for rows.Next() {
		var tableName, engine string
		var tableRows sql.NullInt64
		var totalSize sql.NullFloat64
		if err := rows.Scan(&tableName, &engine, &tableRows, &totalSize); err != nil {
			return err
		}
		rowCount := int64(0)
		if tableRows.Valid {
			rowCount = tableRows.Int64
		}
		totalRows += rowCount
		size := float64(0)
		if totalSize.Valid {
			size = totalSize.Float64
		}
		fmt.Printf("  %-35s %-12s %12s %12s\n", tableName, engine, formatNumber(rowCount), formatBytes(size))
	}
	fmt.Printf("\n  %s %s\n", yellow("Total Estimated Rows:"), green(formatNumber(totalRows)))

	// Index information
	fmt.Println()
	fmt.Printf("  %s\n\n", bold("🔑 Indexes"))

	indexRows, err := db.Query(`
		SELECT
			table_name,
			index_name,
			GROUP_CONCAT(column_name ORDER BY seq_in_index) as columns,
			non_unique
		FROM information_schema.statistics
		WHERE table_schema = ?
		GROUP BY table_name, index_name, non_unique
		ORDER BY table_name, index_name
	`, database)
	if err != nil {
		return fmt.Errorf("failed to get index info: %w", err)
	}
	defer indexRows.Close()

	fmt.Printf("  %-25s %-30s %-30s %s\n", bold("TABLE"), bold("INDEX"), bold("COLUMNS"), bold("UNIQUE"))
	fmt.Printf("  %s\n", dim(strings.Repeat("─", 95)))

	for indexRows.Next() {
		var tableName, indexName, columns string
		var nonUnique int
		if err := indexRows.Scan(&tableName, &indexName, &columns, &nonUnique); err != nil {
			return err
		}
		unique := green("Yes")
		if nonUnique == 1 {
			unique = "No"
		}
		fmt.Printf("  %-25s %-30s %-30s %s\n", truncate(tableName, 25), truncate(indexName, 30), truncate(columns, 30), unique)
	}

	// Foreign keys
	fmt.Println()
	fmt.Printf("  %s\n\n", bold("🔗 Foreign Keys"))

	fkRows, err := db.Query(`
		SELECT
			table_name,
			column_name,
			constraint_name,
			referenced_table_name,
			referenced_column_name
		FROM information_schema.key_column_usage
		WHERE table_schema = ?
			AND referenced_table_name IS NOT NULL
		ORDER BY table_name, constraint_name
	`, database)
	if err != nil {
		return fmt.Errorf("failed to get foreign key info: %w", err)
	}
	defer fkRows.Close()

	fmt.Printf("  %-30s %-35s %-20s %s\n", bold("TABLE.COLUMN"), bold("CONSTRAINT"), bold("REFERENCES"), bold("COLUMN"))
	fmt.Printf("  %s\n", dim(strings.Repeat("─", 95)))

	hasFKs := false
	for fkRows.Next() {
		hasFKs = true
		var tableName, columnName, constraintName, refTable, refColumn string
		if err := fkRows.Scan(&tableName, &columnName, &constraintName, &refTable, &refColumn); err != nil {
			return err
		}
		tableCol := truncate(tableName+"."+columnName, 30)
		fmt.Printf("  %-30s %-35s %-20s %s\n", tableCol, truncate(constraintName, 35), truncate(refTable, 20), refColumn)
	}
	if !hasFKs {
		fmt.Printf("  %s\n", dim("No foreign keys found"))
	}

	// Connection stats
	fmt.Println()
	fmt.Printf("  %s\n\n", bold("📡 Connection Statistics"))

	statsRows, err := db.Query(`SHOW STATUS WHERE Variable_name IN ('Threads_connected', 'Max_used_connections', 'Connections', 'Aborted_connects')`)
	if err != nil {
		return fmt.Errorf("failed to get connection stats: %w", err)
	}
	defer statsRows.Close()

	for statsRows.Next() {
		var name, value string
		if err := statsRows.Scan(&name, &value); err != nil {
			return err
		}
		fmt.Printf("  %-25s %s\n", cyan(name+":"), value)
	}

	fmt.Println()
	return nil
}

func formatBytes(bytes float64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%.0f B", bytes)
	}
	div, exp := float64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", bytes/div, "KMGTPE"[exp])
}

func formatNumber(n int64) string {
	if n < 1000 {
		return strconv.FormatInt(n, 10)
	}

	s := strconv.FormatInt(n, 10)
	result := ""
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result += ","
		}
		result += string(c)
	}
	return result
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

func formatDuration(seconds int64) string {
	days := seconds / 86400
	hours := (seconds % 86400) / 3600
	minutes := (seconds % 3600) / 60

	if days > 0 {
		return fmt.Sprintf("%dd %dh %dm", days, hours, minutes)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	return fmt.Sprintf("%dm", minutes)
}
