package dialect

import (
	"fmt"
	"strings"
)

// PostgresDialect implements Dialect for PostgreSQL databases
type PostgresDialect struct{}

// Name returns "pgsql"
func (d *PostgresDialect) Name() string {
	return "pgsql"
}

// QuoteIdentifier wraps the identifier in double quotes
func (d *PostgresDialect) QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// QuoteLiteral wraps the value in single quotes with escaping
func (d *PostgresDialect) QuoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// Placeholder returns "$N" where N is the position
func (d *PostgresDialect) Placeholder(position int) string {
	return fmt.Sprintf("$%d", position)
}

// PlaceholderStyle returns PlaceholderPositional for PostgreSQL
func (d *PostgresDialect) PlaceholderStyle() PlaceholderStyle {
	return PlaceholderPositional
}

// DisableFKChecksSQL returns the PostgreSQL command to disable FK checks
func (d *PostgresDialect) DisableFKChecksSQL() string {
	return "SET session_replication_role = replica"
}

// EnableFKChecksSQL returns the PostgreSQL command to enable FK checks
func (d *PostgresDialect) EnableFKChecksSQL() string {
	return "SET session_replication_role = DEFAULT"
}

// SupportsSequences returns true for PostgreSQL
func (d *PostgresDialect) SupportsSequences() bool {
	return true
}

// DefaultFKAction returns "NO ACTION" for PostgreSQL
func (d *PostgresDialect) DefaultFKAction() string {
	return "NO ACTION"
}
