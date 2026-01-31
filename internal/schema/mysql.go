package schema

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

// MySQLExtractor extracts schema from MySQL databases
type MySQLExtractor struct{}

// ExtractTables extracts all table schemas from the database
func (e *MySQLExtractor) ExtractTables(db *sql.DB, database string) ([]TableSchema, error) {
	// Get table names
	rows, err := db.Query(`
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = ? AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`, database)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var tables []TableSchema
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}

		table, err := e.extractTable(db, database, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to extract table %s: %w", tableName, err)
		}
		tables = append(tables, table)
	}

	return tables, nil
}

// extractTable extracts the schema for a single table
func (e *MySQLExtractor) extractTable(db *sql.DB, database, tableName string) (TableSchema, error) {
	table := TableSchema{
		Name: tableName,
	}

	// Get CREATE TABLE statement
	var createStmt string
	var tblName string
	err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", database, tableName)).Scan(&tblName, &createStmt)
	if err != nil {
		return table, fmt.Errorf("failed to get CREATE TABLE: %w", err)
	}
	table.CreateStmt = createStmt

	// Extract indexes (excluding primary key - handled separately)
	rows, err := db.Query(`
		SELECT index_name, GROUP_CONCAT(column_name ORDER BY seq_in_index) as columns, non_unique
		FROM information_schema.statistics
		WHERE table_schema = ? AND table_name = ? AND index_name != 'PRIMARY'
		GROUP BY index_name, non_unique
	`, database, tableName)
	if err != nil {
		return table, fmt.Errorf("failed to get indexes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var idx IndexDef
		var columns string
		var nonUnique int
		if err := rows.Scan(&idx.Name, &columns, &nonUnique); err != nil {
			return table, err
		}
		idx.Columns = strings.Split(columns, ",")
		idx.IsUnique = nonUnique == 0
		idx.CreateStmt = e.buildCreateIndexStmt(tableName, idx)
		table.Indexes = append(table.Indexes, idx)
	}

	// Extract foreign keys
	rows, err = db.Query(`
		SELECT
			kcu.constraint_name,
			GROUP_CONCAT(kcu.column_name ORDER BY kcu.ordinal_position) as columns,
			kcu.referenced_table_name,
			GROUP_CONCAT(kcu.referenced_column_name ORDER BY kcu.ordinal_position) as ref_columns,
			rc.delete_rule,
			rc.update_rule
		FROM information_schema.key_column_usage kcu
		JOIN information_schema.referential_constraints rc
			ON kcu.constraint_name = rc.constraint_name
			AND kcu.table_schema = rc.constraint_schema
		WHERE kcu.table_schema = ?
			AND kcu.table_name = ?
			AND kcu.referenced_table_name IS NOT NULL
		GROUP BY kcu.constraint_name, kcu.referenced_table_name, rc.delete_rule, rc.update_rule
	`, database, tableName)
	if err != nil {
		return table, fmt.Errorf("failed to get foreign keys: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var fk ForeignKeyDef
		var columns, refColumns string
		if err := rows.Scan(&fk.Name, &columns, &fk.RefTable, &refColumns, &fk.OnDelete, &fk.OnUpdate); err != nil {
			return table, err
		}
		fk.Columns = strings.Split(columns, ",")
		fk.RefColumns = strings.Split(refColumns, ",")
		fk.ConstraintStmt = e.buildAddForeignKeyStmt(tableName, fk)
		table.ForeignKeys = append(table.ForeignKeys, fk)
	}

	return table, nil
}

// buildCreateIndexStmt builds a CREATE INDEX statement
func (e *MySQLExtractor) buildCreateIndexStmt(tableName string, idx IndexDef) string {
	uniqueStr := ""
	if idx.IsUnique {
		uniqueStr = "UNIQUE "
	}
	quotedCols := make([]string, len(idx.Columns))
	for i, col := range idx.Columns {
		quotedCols[i] = "`" + col + "`"
	}
	return fmt.Sprintf("CREATE %sINDEX `%s` ON `%s` (%s)",
		uniqueStr, idx.Name, tableName, strings.Join(quotedCols, ", "))
}

// buildAddForeignKeyStmt builds an ALTER TABLE ADD CONSTRAINT statement for a foreign key
func (e *MySQLExtractor) buildAddForeignKeyStmt(tableName string, fk ForeignKeyDef) string {
	quotedCols := make([]string, len(fk.Columns))
	for i, col := range fk.Columns {
		quotedCols[i] = "`" + col + "`"
	}
	quotedRefCols := make([]string, len(fk.RefColumns))
	for i, col := range fk.RefColumns {
		quotedRefCols[i] = "`" + col + "`"
	}
	stmt := fmt.Sprintf("ALTER TABLE `%s` ADD CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s` (%s)",
		tableName, fk.Name, strings.Join(quotedCols, ", "), fk.RefTable, strings.Join(quotedRefCols, ", "))
	if fk.OnDelete != "" && fk.OnDelete != "RESTRICT" {
		stmt += " ON DELETE " + fk.OnDelete
	}
	if fk.OnUpdate != "" && fk.OnUpdate != "RESTRICT" {
		stmt += " ON UPDATE " + fk.OnUpdate
	}
	return stmt
}

// ExtractViews extracts all view definitions
func (e *MySQLExtractor) ExtractViews(db *sql.DB, database string) ([]ViewDef, error) {
	rows, err := db.Query(`
		SELECT table_name, view_definition
		FROM information_schema.views
		WHERE table_schema = ?
		ORDER BY table_name
	`, database)
	if err != nil {
		return nil, fmt.Errorf("failed to list views: %w", err)
	}
	defer rows.Close()

	var views []ViewDef
	for rows.Next() {
		var view ViewDef
		var viewDef string
		if err := rows.Scan(&view.Name, &viewDef); err != nil {
			return nil, err
		}

		// Get full CREATE VIEW statement
		var createStmt string
		var vName, charSet, collation string
		err := db.QueryRow(fmt.Sprintf("SHOW CREATE VIEW `%s`.`%s`", database, view.Name)).
			Scan(&vName, &createStmt, &charSet, &collation)
		if err != nil {
			return nil, fmt.Errorf("failed to get CREATE VIEW for %s: %w", view.Name, err)
		}
		view.CreateStmt = createStmt
		view.Dependencies = extractViewDependencies(viewDef)
		views = append(views, view)
	}

	// Sort views by dependency order
	views = sortViewsByDependency(views)

	return views, nil
}

// ExtractSequences is a no-op for MySQL (MySQL doesn't have sequences)
func (e *MySQLExtractor) ExtractSequences(db *sql.DB, database string) ([]SequenceDef, error) {
	return nil, nil
}

// MySQLApplier applies schema to MySQL databases
type MySQLApplier struct{}

// CreateTable creates a table in the database
func (a *MySQLApplier) CreateTable(db *sql.DB, table TableSchema) error {
	// Remove foreign key constraints from CREATE TABLE statement
	// We'll add them later after all tables are created
	createStmt := removeForeignKeysFromCreateTable(table.CreateStmt)

	_, err := db.Exec(createStmt)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", table.Name, err)
	}
	return nil
}

// CreateIndex creates an index on a table
func (a *MySQLApplier) CreateIndex(db *sql.DB, index IndexDef) error {
	_, err := db.Exec(index.CreateStmt)
	if err != nil {
		return fmt.Errorf("failed to create index %s: %w", index.Name, err)
	}
	return nil
}

// CreateForeignKey adds a foreign key constraint to a table
func (a *MySQLApplier) CreateForeignKey(db *sql.DB, fk ForeignKeyDef) error {
	_, err := db.Exec(fk.ConstraintStmt)
	if err != nil {
		return fmt.Errorf("failed to create foreign key %s: %w", fk.Name, err)
	}
	return nil
}

// CreateView creates a view in the database
func (a *MySQLApplier) CreateView(db *sql.DB, view ViewDef) error {
	_, err := db.Exec(view.CreateStmt)
	if err != nil {
		return fmt.Errorf("failed to create view %s: %w", view.Name, err)
	}
	return nil
}

// CreateSequence is a no-op for MySQL
func (a *MySQLApplier) CreateSequence(db *sql.DB, seq SequenceDef) error {
	return nil
}

// SetSequenceValue is a no-op for MySQL
func (a *MySQLApplier) SetSequenceValue(db *sql.DB, seq SequenceDef) error {
	return nil
}

// removeForeignKeysFromCreateTable removes FOREIGN KEY constraints from a CREATE TABLE statement
func removeForeignKeysFromCreateTable(createStmt string) string {
	// This regex matches CONSTRAINT ... FOREIGN KEY ... REFERENCES ... lines
	// Including ON DELETE and ON UPDATE clauses
	re := regexp.MustCompile(`,?\s*CONSTRAINT\s+`+"`[^`]+`"+`\s+FOREIGN KEY[^,)]+(?:ON DELETE [^,)]+)?(?:ON UPDATE [^,)]+)?`)
	result := re.ReplaceAllString(createStmt, "")

	// Also handle FOREIGN KEY without CONSTRAINT name
	re2 := regexp.MustCompile(`,?\s*FOREIGN KEY[^,)]+(?:ON DELETE [^,)]+)?(?:ON UPDATE [^,)]+)?`)
	result = re2.ReplaceAllString(result, "")

	return result
}

// extractViewDependencies extracts table/view names referenced in a view definition
func extractViewDependencies(viewDef string) []string {
	// Simple extraction - look for FROM and JOIN clauses
	re := regexp.MustCompile(`(?i)(?:FROM|JOIN)\s+` + "`?([a-zA-Z_][a-zA-Z0-9_]*)`?")
	matches := re.FindAllStringSubmatch(viewDef, -1)

	deps := make(map[string]bool)
	for _, match := range matches {
		if len(match) > 1 {
			deps[match[1]] = true
		}
	}

	var result []string
	for dep := range deps {
		result = append(result, dep)
	}
	return result
}

// sortViewsByDependency sorts views so that dependencies come before dependents
func sortViewsByDependency(views []ViewDef) []ViewDef {
	// Build dependency graph
	viewSet := make(map[string]bool)
	for _, v := range views {
		viewSet[v.Name] = true
	}

	// Simple topological sort
	sorted := make([]ViewDef, 0, len(views))
	added := make(map[string]bool)

	for len(sorted) < len(views) {
		for _, v := range views {
			if added[v.Name] {
				continue
			}
			// Check if all view dependencies are either not views or already added
			canAdd := true
			for _, dep := range v.Dependencies {
				if viewSet[dep] && !added[dep] {
					canAdd = false
					break
				}
			}
			if canAdd {
				sorted = append(sorted, v)
				added[v.Name] = true
			}
		}
		// Prevent infinite loop if there's a circular dependency
		if len(sorted) == 0 {
			return views
		}
	}

	return sorted
}
