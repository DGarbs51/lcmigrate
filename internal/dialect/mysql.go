package dialect

import "strings"

// MySQLDialect implements Dialect for MySQL databases
type MySQLDialect struct{}

// Name returns "mysql"
func (d *MySQLDialect) Name() string {
	return "mysql"
}

// QuoteIdentifier wraps the identifier in backticks
func (d *MySQLDialect) QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// QuoteLiteral wraps the value in single quotes with escaping
func (d *MySQLDialect) QuoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// Placeholder always returns "?" for MySQL
func (d *MySQLDialect) Placeholder(position int) string {
	return "?"
}

// PlaceholderStyle returns PlaceholderQuestion for MySQL
func (d *MySQLDialect) PlaceholderStyle() PlaceholderStyle {
	return PlaceholderQuestion
}

// DisableFKChecksSQL returns the MySQL command to disable FK checks
func (d *MySQLDialect) DisableFKChecksSQL() string {
	return "SET FOREIGN_KEY_CHECKS = 0"
}

// EnableFKChecksSQL returns the MySQL command to enable FK checks
func (d *MySQLDialect) EnableFKChecksSQL() string {
	return "SET FOREIGN_KEY_CHECKS = 1"
}

// SupportsSequences returns false for MySQL (uses AUTO_INCREMENT instead)
func (d *MySQLDialect) SupportsSequences() bool {
	return false
}

// DefaultFKAction returns "RESTRICT" for MySQL
func (d *MySQLDialect) DefaultFKAction() string {
	return "RESTRICT"
}
