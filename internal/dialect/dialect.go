package dialect

// PlaceholderStyle indicates how SQL parameters are specified
type PlaceholderStyle int

const (
	// PlaceholderQuestion uses ? for all parameters (MySQL)
	PlaceholderQuestion PlaceholderStyle = iota
	// PlaceholderPositional uses $1, $2, etc. (PostgreSQL)
	PlaceholderPositional
)

// Dialect defines engine-specific SQL syntax rules
type Dialect interface {
	// Name returns the dialect identifier ("mysql" or "pgsql")
	Name() string

	// QuoteIdentifier quotes a table/column name for the engine
	// MySQL: `identifier`, PostgreSQL: "identifier"
	QuoteIdentifier(name string) string

	// QuoteLiteral quotes a string literal value
	QuoteLiteral(value string) string

	// Placeholder returns the parameter placeholder for the given position (1-indexed)
	// MySQL: always "?", PostgreSQL: "$1", "$2", etc.
	Placeholder(position int) string

	// PlaceholderStyle indicates if placeholders are positional
	PlaceholderStyle() PlaceholderStyle

	// DisableFKChecksSQL returns the SQL to disable foreign key checks
	DisableFKChecksSQL() string

	// EnableFKChecksSQL returns the SQL to enable foreign key checks
	EnableFKChecksSQL() string

	// SupportsSequences returns true if the dialect supports sequences (PostgreSQL)
	SupportsSequences() bool

	// DefaultFKAction returns the default ON DELETE/UPDATE action
	// MySQL: "RESTRICT", PostgreSQL: "NO ACTION"
	DefaultFKAction() string
}

// New returns the appropriate dialect for the engine name
func New(engine string) Dialect {
	switch engine {
	case "mysql":
		return &MySQLDialect{}
	case "pgsql":
		return &PostgresDialect{}
	default:
		return nil
	}
}
