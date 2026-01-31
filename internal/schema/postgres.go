package schema

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

// PostgresExtractor extracts schema from PostgreSQL databases
type PostgresExtractor struct{}

// ExtractTables extracts all table schemas from the database
func (e *PostgresExtractor) ExtractTables(db *sql.DB, database string) ([]TableSchema, error) {
	rows, err := db.Query(`
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`)
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

		table, err := e.extractTable(db, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to extract table %s: %w", tableName, err)
		}
		tables = append(tables, table)
	}

	return tables, nil
}

// extractTable extracts the schema for a single table
func (e *PostgresExtractor) extractTable(db *sql.DB, tableName string) (TableSchema, error) {
	table := TableSchema{
		Name: tableName,
	}

	// Build CREATE TABLE statement from column information
	createStmt, err := e.buildCreateTableStmt(db, tableName)
	if err != nil {
		return table, err
	}
	table.CreateStmt = createStmt

	// Extract indexes (excluding primary key)
	rows, err := db.Query(`
		SELECT
			i.relname as index_name,
			array_to_string(array_agg(a.attname ORDER BY k.n), ',') as columns,
			ix.indisunique as is_unique,
			pg_get_indexdef(ix.indexrelid) as index_def
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, n) ON true
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
		WHERE t.relname = $1
			AND n.nspname = 'public'
			AND NOT ix.indisprimary
		GROUP BY i.relname, ix.indisunique, ix.indexrelid
		ORDER BY i.relname
	`, tableName)
	if err != nil {
		return table, fmt.Errorf("failed to get indexes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var idx IndexDef
		var columns string
		if err := rows.Scan(&idx.Name, &columns, &idx.IsUnique, &idx.CreateStmt); err != nil {
			return table, err
		}
		idx.Columns = strings.Split(columns, ",")
		table.Indexes = append(table.Indexes, idx)
	}

	// Extract foreign keys
	rows, err = db.Query(`
		SELECT
			tc.constraint_name,
			string_agg(kcu.column_name, ',' ORDER BY kcu.ordinal_position) as columns,
			ccu.table_name as ref_table,
			string_agg(ccu.column_name, ',' ORDER BY kcu.ordinal_position) as ref_columns,
			rc.delete_rule,
			rc.update_rule
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage ccu
			ON tc.constraint_name = ccu.constraint_name
			AND tc.table_schema = ccu.table_schema
		JOIN information_schema.referential_constraints rc
			ON tc.constraint_name = rc.constraint_name
			AND tc.table_schema = rc.constraint_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
			AND tc.table_name = $1
			AND tc.table_schema = 'public'
		GROUP BY tc.constraint_name, ccu.table_name, rc.delete_rule, rc.update_rule
	`, tableName)
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

// buildCreateTableStmt builds a CREATE TABLE statement from pg_catalog information
func (e *PostgresExtractor) buildCreateTableStmt(db *sql.DB, tableName string) (string, error) {
	// Get column definitions
	rows, err := db.Query(`
		SELECT
			a.attname as column_name,
			pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type,
			COALESCE(pg_get_expr(d.adbin, d.adrelid), '') as column_default,
			a.attnotnull as not_null,
			CASE WHEN a.attidentity != '' THEN true ELSE false END as is_identity
		FROM pg_catalog.pg_attribute a
		LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid, a.attnum) = (d.adrelid, d.adnum)
		WHERE a.attrelid = $1::regclass
			AND a.attnum > 0
			AND NOT a.attisdropped
		ORDER BY a.attnum
	`, tableName)
	if err != nil {
		return "", fmt.Errorf("failed to get columns: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var colName, dataType, colDefault string
		var notNull, isIdentity bool
		if err := rows.Scan(&colName, &dataType, &colDefault, &notNull, &isIdentity); err != nil {
			return "", err
		}

		// Convert integer + nextval default to SERIAL/BIGSERIAL types
		// This avoids needing to create sequences separately
		if strings.Contains(colDefault, "nextval(") {
			switch dataType {
			case "bigint":
				dataType = "BIGSERIAL"
			case "integer":
				dataType = "SERIAL"
			case "smallint":
				dataType = "SMALLSERIAL"
			}
			colDefault = "" // SERIAL types handle their own sequence
		}

		colDef := fmt.Sprintf("    %s %s", quoteIdentifier(colName), dataType)
		if colDefault != "" && !isIdentity {
			colDef += " DEFAULT " + colDefault
		}
		if notNull && !strings.Contains(dataType, "SERIAL") {
			// SERIAL types are implicitly NOT NULL
			colDef += " NOT NULL"
		}
		columns = append(columns, colDef)
	}

	// Get primary key
	var pkColumns string
	err = db.QueryRow(`
		SELECT string_agg(a.attname, ', ' ORDER BY array_position(i.indkey, a.attnum))
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary
	`, tableName).Scan(&pkColumns)
	if err != nil && err != sql.ErrNoRows {
		return "", fmt.Errorf("failed to get primary key: %w", err)
	}

	var pkConstraint string
	if pkColumns != "" {
		pkConstraint = fmt.Sprintf(",\n    PRIMARY KEY (%s)", pkColumns)
	}

	stmt := fmt.Sprintf("CREATE TABLE %s (\n%s%s\n)",
		quoteIdentifier(tableName),
		strings.Join(columns, ",\n"),
		pkConstraint)

	return stmt, nil
}

// buildAddForeignKeyStmt builds an ALTER TABLE ADD CONSTRAINT statement
func (e *PostgresExtractor) buildAddForeignKeyStmt(tableName string, fk ForeignKeyDef) string {
	quotedCols := make([]string, len(fk.Columns))
	for i, col := range fk.Columns {
		quotedCols[i] = quoteIdentifier(col)
	}
	quotedRefCols := make([]string, len(fk.RefColumns))
	for i, col := range fk.RefColumns {
		quotedRefCols[i] = quoteIdentifier(col)
	}

	stmt := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
		quoteIdentifier(tableName),
		quoteIdentifier(fk.Name),
		strings.Join(quotedCols, ", "),
		quoteIdentifier(fk.RefTable),
		strings.Join(quotedRefCols, ", "))

	if fk.OnDelete != "" && fk.OnDelete != "NO ACTION" {
		stmt += " ON DELETE " + fk.OnDelete
	}
	if fk.OnUpdate != "" && fk.OnUpdate != "NO ACTION" {
		stmt += " ON UPDATE " + fk.OnUpdate
	}

	return stmt
}

// ExtractViews extracts all view definitions
func (e *PostgresExtractor) ExtractViews(db *sql.DB, database string) ([]ViewDef, error) {
	rows, err := db.Query(`
		SELECT viewname, pg_get_viewdef(viewname::regclass, true) as view_def
		FROM pg_views
		WHERE schemaname = 'public'
		ORDER BY viewname
	`)
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
		view.CreateStmt = fmt.Sprintf("CREATE VIEW %s AS\n%s", quoteIdentifier(view.Name), viewDef)
		view.Dependencies = extractPgViewDependencies(viewDef)
		views = append(views, view)
	}

	// Sort by dependency order
	views = sortViewsByDependency(views)

	return views, nil
}

// ExtractSequences extracts all sequence definitions
func (e *PostgresExtractor) ExtractSequences(db *sql.DB, database string) ([]SequenceDef, error) {
	// Use pg_catalog directly to avoid information_schema compatibility issues
	// with some PostgreSQL providers (e.g., Neon, Laravel Cloud)
	rows, err := db.Query(`
		SELECT
			s.sequencename,
			d.refobjid::regclass::text as owned_by,
			COALESCE(s.last_value, 1) as current_val
		FROM pg_sequences s
		LEFT JOIN pg_depend d ON d.objid = (s.schemaname || '.' || s.sequencename)::regclass
			AND d.deptype = 'a'
			AND d.classid = 'pg_class'::regclass
		WHERE s.schemaname = 'public'
		ORDER BY s.sequencename
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list sequences: %w", err)
	}
	defer rows.Close()

	var sequences []SequenceDef
	for rows.Next() {
		var seq SequenceDef
		var ownedBy sql.NullString
		if err := rows.Scan(&seq.Name, &ownedBy, &seq.CurrentVal); err != nil {
			return nil, err
		}
		seq.OwnedBy = ownedBy.String
		seq.CreateStmt = fmt.Sprintf("CREATE SEQUENCE %s", quoteIdentifier(seq.Name))
		sequences = append(sequences, seq)
	}

	return sequences, nil
}

// PostgresApplier applies schema to PostgreSQL databases
type PostgresApplier struct{}

// CreateTable creates a table in the database
func (a *PostgresApplier) CreateTable(db *sql.DB, table TableSchema) error {
	_, err := db.Exec(table.CreateStmt)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", table.Name, err)
	}
	return nil
}

// CreateIndex creates an index on a table
func (a *PostgresApplier) CreateIndex(db *sql.DB, index IndexDef) error {
	_, err := db.Exec(index.CreateStmt)
	if err != nil {
		return fmt.Errorf("failed to create index %s: %w", index.Name, err)
	}
	return nil
}

// CreateForeignKey adds a foreign key constraint
func (a *PostgresApplier) CreateForeignKey(db *sql.DB, fk ForeignKeyDef) error {
	_, err := db.Exec(fk.ConstraintStmt)
	if err != nil {
		return fmt.Errorf("failed to create foreign key %s: %w", fk.Name, err)
	}
	return nil
}

// CreateView creates a view
func (a *PostgresApplier) CreateView(db *sql.DB, view ViewDef) error {
	_, err := db.Exec(view.CreateStmt)
	if err != nil {
		return fmt.Errorf("failed to create view %s: %w", view.Name, err)
	}
	return nil
}

// CreateSequence creates a sequence
func (a *PostgresApplier) CreateSequence(db *sql.DB, seq SequenceDef) error {
	// Sequences are typically created automatically with SERIAL/BIGSERIAL columns
	// This handles standalone sequences
	_, err := db.Exec(seq.CreateStmt)
	if err != nil {
		// Ignore if already exists (likely auto-created)
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create sequence %s: %w", seq.Name, err)
		}
	}
	return nil
}

// SetSequenceValue sets the current value of a sequence
func (a *PostgresApplier) SetSequenceValue(db *sql.DB, seq SequenceDef) error {
	_, err := db.Exec(fmt.Sprintf("SELECT setval(%s, $1, true)",
		quoteLiteral(seq.Name)), seq.CurrentVal)
	if err != nil {
		return fmt.Errorf("failed to set sequence value for %s: %w", seq.Name, err)
	}
	return nil
}

// quoteIdentifier quotes a PostgreSQL identifier
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// quoteLiteral quotes a PostgreSQL string literal
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// extractPgViewDependencies extracts table/view names from a view definition
func extractPgViewDependencies(viewDef string) []string {
	re := regexp.MustCompile(`(?i)(?:FROM|JOIN)\s+"?([a-zA-Z_][a-zA-Z0-9_]*)"?`)
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
