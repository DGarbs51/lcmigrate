package db

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/DGarbs51/lcmigrate/internal/format"
	"github.com/fatih/color"
)

func AnalyzePostgres(db *sql.DB, database string) error {
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
	if err := db.QueryRow(`SELECT version()`).Scan(&version); err != nil {
		return fmt.Errorf("failed to get version: %w", err)
	}
	fmt.Printf("  %-22s %s\n", cyan("Version:"), green(format.Truncate(version, 60)))

	var maxConns string
	if err := db.QueryRow(`SHOW max_connections`).Scan(&maxConns); err == nil {
		fmt.Printf("  %-22s %s\n", cyan("Max Connections:"), maxConns)
	}

	var sharedBuffers string
	if err := db.QueryRow(`SHOW shared_buffers`).Scan(&sharedBuffers); err == nil {
		fmt.Printf("  %-22s %s\n", cyan("Shared Buffers:"), sharedBuffers)
	}

	var workMem string
	if err := db.QueryRow(`SHOW work_mem`).Scan(&workMem); err == nil {
		fmt.Printf("  %-22s %s\n", cyan("Work Memory:"), workMem)
	}

	var encoding string
	if err := db.QueryRow(`SHOW server_encoding`).Scan(&encoding); err == nil {
		fmt.Printf("  %-22s %s\n", cyan("Encoding:"), encoding)
	}

	var timezone string
	if err := db.QueryRow(`SHOW timezone`).Scan(&timezone); err == nil {
		fmt.Printf("  %-22s %s\n", cyan("Timezone:"), timezone)
	}

	// Uptime
	var uptime float64
	if err := db.QueryRow(`SELECT EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time()))`).Scan(&uptime); err == nil {
		fmt.Printf("  %-22s %s\n", cyan("Uptime:"), format.Duration(int64(uptime)))
	}

	// Table count
	var tableCount int
	err := db.QueryRow(`
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
	`).Scan(&tableCount)
	if err != nil {
		return fmt.Errorf("failed to get table count: %w", err)
	}

	// Database size
	var dbSize int64
	err = db.QueryRow(`SELECT pg_database_size(current_database())`).Scan(&dbSize)
	if err != nil {
		return fmt.Errorf("failed to get database size: %w", err)
	}

	// Summary box
	fmt.Println()
	fmt.Printf("  %s\n", bold("📊 Database Summary"))
	fmt.Printf("  %s\n\n", dim("─────────────────────────────"))
	fmt.Printf("  %s  %s\n", cyan("Tables:"), green(strconv.Itoa(tableCount)))
	fmt.Printf("  %s    %s\n", cyan("Size:"), green(format.Bytes(float64(dbSize))))

	// Table details
	fmt.Println()
	fmt.Printf("  %s\n\n", bold("📋 Table Details"))

	rows, err := db.Query(`
		SELECT
			t.table_name,
			COALESCE(s.n_live_tup, 0) as row_count,
			pg_total_relation_size(quote_ident(t.table_name)::regclass) as total_size
		FROM information_schema.tables t
		LEFT JOIN pg_stat_user_tables s ON t.table_name = s.relname
		WHERE t.table_schema = 'public' AND t.table_type = 'BASE TABLE'
		ORDER BY t.table_name
	`)
	if err != nil {
		return fmt.Errorf("failed to get table details: %w", err)
	}
	defer rows.Close()

	fmt.Printf("  %-35s %12s %12s\n", bold("TABLE"), bold("ROW COUNT"), bold("SIZE"))
	fmt.Printf("  %s\n", dim(strings.Repeat("─", 63)))

	var totalRows int64
	for rows.Next() {
		var tableName string
		var rowCount, totalSize int64
		if err := rows.Scan(&tableName, &rowCount, &totalSize); err != nil {
			return err
		}
		totalRows += rowCount
		fmt.Printf("  %-35s %12s %12s\n", format.Truncate(tableName, 35), format.Number(rowCount), format.Bytes(float64(totalSize)))
	}
	fmt.Printf("\n  %s %s\n", yellow("Total Rows:"), green(format.Number(totalRows)))

	// Index information
	fmt.Println()
	fmt.Printf("  %s\n\n", bold("🔑 Indexes"))

	indexRows, err := db.Query(`
		SELECT
			tablename,
			indexname,
			indexdef LIKE 'CREATE UNIQUE%' as is_unique
		FROM pg_indexes
		WHERE schemaname = 'public'
		ORDER BY tablename, indexname
	`)
	if err != nil {
		return fmt.Errorf("failed to get index info: %w", err)
	}
	defer indexRows.Close()

	fmt.Printf("  %-30s %-40s %s\n", bold("TABLE"), bold("INDEX"), bold("UNIQUE"))
	fmt.Printf("  %s\n", dim(strings.Repeat("─", 78)))

	for indexRows.Next() {
		var tableName, indexName string
		var isUnique bool
		if err := indexRows.Scan(&tableName, &indexName, &isUnique); err != nil {
			return err
		}
		unique := "No"
		if isUnique {
			unique = green("Yes")
		}
		fmt.Printf("  %-30s %-40s %s\n", format.Truncate(tableName, 30), format.Truncate(indexName, 40), unique)
	}

	// Foreign keys
	fmt.Println()
	fmt.Printf("  %s\n\n", bold("🔗 Foreign Keys"))

	fkRows, err := db.Query(`
		SELECT
			tc.table_name,
			kcu.column_name,
			tc.constraint_name,
			ccu.table_name AS foreign_table_name,
			ccu.column_name AS foreign_column_name
		FROM information_schema.table_constraints AS tc
		JOIN information_schema.key_column_usage AS kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage AS ccu
			ON ccu.constraint_name = tc.constraint_name
			AND ccu.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
			AND tc.table_schema = 'public'
		ORDER BY tc.table_name, tc.constraint_name
	`)
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
		tableCol := format.Truncate(tableName+"."+columnName, 30)
		fmt.Printf("  %-30s %-35s %-20s %s\n", tableCol, format.Truncate(constraintName, 35), format.Truncate(refTable, 20), refColumn)
	}
	if !hasFKs {
		fmt.Printf("  %s\n", dim("No foreign keys found"))
	}

	// Connection stats
	fmt.Println()
	fmt.Printf("  %s\n\n", bold("📡 Connection Statistics"))

	var activeConns, idleConns, totalConns int

	err = db.QueryRow(`SELECT COUNT(*) FROM pg_stat_activity WHERE state = 'active'`).Scan(&activeConns)
	if err != nil {
		return fmt.Errorf("failed to get active connections: %w", err)
	}

	err = db.QueryRow(`SELECT COUNT(*) FROM pg_stat_activity WHERE state = 'idle'`).Scan(&idleConns)
	if err != nil {
		return fmt.Errorf("failed to get idle connections: %w", err)
	}

	err = db.QueryRow(`SELECT COUNT(*) FROM pg_stat_activity`).Scan(&totalConns)
	if err != nil {
		return fmt.Errorf("failed to get total connections: %w", err)
	}

	fmt.Printf("  %-22s %s\n", cyan("Active Connections:"), green(strconv.Itoa(activeConns)))
	fmt.Printf("  %-22s %s\n", cyan("Idle Connections:"), strconv.Itoa(idleConns))
	fmt.Printf("  %-22s %s\n", cyan("Total Connections:"), strconv.Itoa(totalConns))

	fmt.Println()
	return nil
}
