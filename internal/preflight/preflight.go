package preflight

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/DGarbs51/lcmigrate/internal/config"
	"github.com/DGarbs51/lcmigrate/internal/prompt"
	"github.com/DGarbs51/lcmigrate/internal/ui"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// CheckResult represents the result of a single pre-flight check
type CheckResult struct {
	Name    string
	Passed  bool
	Message string
	Warning bool
}

// PreflightResult contains all pre-flight check results
type PreflightResult struct {
	SourceConn   *sql.DB
	DestConn     *sql.DB
	SourceInfo   DatabaseInfo
	DestInfo     DatabaseInfo
	Checks       []CheckResult
	Passed       bool
	Aborted      bool
}

// DatabaseInfo contains database metadata
type DatabaseInfo struct {
	Version      string
	MajorVersion int
	TableCount   int
	ViewCount    int
	TotalSize    int64
	Tables       []string
}

// ConnectResult contains the connection and metadata about how it was established
type ConnectResult struct {
	DB      *sql.DB
	SSLMode string // For PostgreSQL: which SSL mode was used
}

// DatabaseNotExistsError indicates the target database doesn't exist
type DatabaseNotExistsError struct {
	Database string
}

func (e *DatabaseNotExistsError) Error() string {
	return fmt.Sprintf("database %q does not exist", e.Database)
}

// isDatabaseNotExistsError checks if the error indicates the database doesn't exist
func isDatabaseNotExistsError(err error, engine string) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	switch engine {
	case "pgsql":
		// PostgreSQL error code 3D000: invalid_catalog_name (database does not exist)
		return strings.Contains(errStr, "3D000") || strings.Contains(errStr, "does not exist")
	case "mysql":
		// MySQL error 1049: Unknown database
		return strings.Contains(errStr, "1049") || strings.Contains(errStr, "Unknown database")
	}
	return false
}

// Connect establishes a database connection
func Connect(cfg config.DatabaseConfig) (*ConnectResult, error) {
	switch cfg.Engine {
	case "mysql":
		return connectMySQL(cfg)
	case "pgsql":
		return connectPostgres(cfg)
	default:
		return nil, fmt.Errorf("unsupported database engine: %s", cfg.Engine)
	}
}

// connectMySQL establishes a MySQL connection
func connectMySQL(cfg config.DatabaseConfig) (*ConnectResult, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		if isDatabaseNotExistsError(err, "mysql") {
			return nil, &DatabaseNotExistsError{Database: cfg.Database}
		}
		return nil, err
	}

	return &ConnectResult{DB: db}, nil
}

// connectPostgres establishes a PostgreSQL connection with SSL fallback
// Tries: require -> prefer -> disable
func connectPostgres(cfg config.DatabaseConfig) (*ConnectResult, error) {
	sslModes := []string{"require", "prefer", "disable"}
	var lastErr error
	var dbNotExists bool

	for _, sslMode := range sslModes {
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
			url.QueryEscape(cfg.User), url.QueryEscape(cfg.Password),
			cfg.Host, cfg.Port, cfg.Database, sslMode)

		db, err := sql.Open("postgres", dsn)
		if err != nil {
			lastErr = err
			continue
		}

		if err := db.Ping(); err != nil {
			db.Close()
			lastErr = err
			// Check if database doesn't exist - this error won't be fixed by changing SSL mode
			if isDatabaseNotExistsError(err, "pgsql") {
				dbNotExists = true
				break
			}
			continue
		}

		// Connection successful
		return &ConnectResult{DB: db, SSLMode: sslMode}, nil
	}

	if dbNotExists {
		return nil, &DatabaseNotExistsError{Database: cfg.Database}
	}
	return nil, fmt.Errorf("failed to connect with any SSL mode: %w", lastErr)
}

// CreateDatabase creates the specified database on the target server
func CreateDatabase(cfg config.DatabaseConfig) error {
	switch cfg.Engine {
	case "mysql":
		return createMySQLDatabase(cfg)
	case "pgsql":
		return createPostgresDatabase(cfg)
	default:
		return fmt.Errorf("unsupported database engine: %s", cfg.Engine)
	}
}

// createMySQLDatabase creates a MySQL database
func createMySQLDatabase(cfg config.DatabaseConfig) error {
	// Connect without specifying a database
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/",
		cfg.User, cfg.Password, cfg.Host, cfg.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping server: %w", err)
	}

	// Create the database
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE `%s`", cfg.Database))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	return nil
}

// createPostgresDatabase creates a PostgreSQL database
func createPostgresDatabase(cfg config.DatabaseConfig) error {
	// Connect to the default 'postgres' database
	sslModes := []string{"require", "prefer", "disable"}
	var db *sql.DB
	var lastErr error

	for _, sslMode := range sslModes {
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/postgres?sslmode=%s",
			url.QueryEscape(cfg.User), url.QueryEscape(cfg.Password),
			cfg.Host, cfg.Port, sslMode)

		conn, err := sql.Open("postgres", dsn)
		if err != nil {
			lastErr = err
			continue
		}

		if err := conn.Ping(); err != nil {
			conn.Close()
			lastErr = err
			continue
		}

		db = conn
		break
	}

	if db == nil {
		return fmt.Errorf("failed to connect to server: %w", lastErr)
	}
	defer db.Close()

	// Create the database (use quoted identifier to handle special chars)
	_, err := db.Exec(fmt.Sprintf("CREATE DATABASE %s", quoteIdentifier(cfg.Database)))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	return nil
}

// Run executes all pre-flight checks
func Run(cfg config.MigrationConfig) (*PreflightResult, error) {
	result := &PreflightResult{
		Passed: true,
	}

	ui.Header("Pre-flight Checks")

	// 1. Connect to source
	sourceConnResult, err := Connect(cfg.Source)
	if err != nil {
		result.Checks = append(result.Checks, CheckResult{
			Name:    "Source connection",
			Passed:  false,
			Message: err.Error(),
		})
		ui.Error(fmt.Sprintf("Source connection failed: %s", err))
		result.Passed = false
		return result, nil
	}
	result.SourceConn = sourceConnResult.DB
	result.Checks = append(result.Checks, CheckResult{
		Name:    "Source connection",
		Passed:  true,
		Message: fmt.Sprintf("Connected to %s://%s@%s:%s/%s", cfg.Source.Engine, cfg.Source.User, cfg.Source.Host, cfg.Source.Port, cfg.Source.Database),
	})
	sourceSSLInfo := ""
	if sourceConnResult.SSLMode != "" && sourceConnResult.SSLMode != "require" {
		sourceSSLInfo = fmt.Sprintf(" (SSL: %s)", sourceConnResult.SSLMode)
	}
	ui.Success(fmt.Sprintf("Source connection successful%s", sourceSSLInfo))

	// Get source database info early (needed for dry-run database creation)
	sourceInfo, err := getDatabaseInfo(result.SourceConn, cfg.Source.Engine, cfg.Source.Database)
	if err != nil {
		ui.Error(fmt.Sprintf("Failed to get source database info: %s", err))
		result.Passed = false
		return result, nil
	}
	result.SourceInfo = sourceInfo

	// 2. Connect to destination
	destConnResult, err := Connect(cfg.Destination)
	if err != nil {
		// Check if database doesn't exist and offer to create it
		var dbNotExists *DatabaseNotExistsError
		if errors.As(err, &dbNotExists) {
			ui.Warning(fmt.Sprintf("Database %q does not exist on destination server", cfg.Destination.Database))
			if cfg.DryRun {
				// For dry runs, just pretend we created it and skip the actual connection
				ui.DryRun(fmt.Sprintf("Would create database %q", cfg.Destination.Database))
				result.Checks = append(result.Checks, CheckResult{
					Name:    "Destination connection",
					Passed:  true,
					Message: fmt.Sprintf("Would create database %q", cfg.Destination.Database),
				})
				// Create a mock destination info for dry run
				result.DestInfo = DatabaseInfo{
					Version:      result.SourceInfo.Version,
					MajorVersion: result.SourceInfo.MajorVersion,
					TableCount:   0,
					ViewCount:    0,
					TotalSize:    0,
				}
				// Skip the rest of the destination checks for dry run
				ui.Success("Destination database is empty (will be created)")
				ui.Success(fmt.Sprintf("Source database size: %s (%d tables, %d views)",
					ui.FormatBytes(float64(result.SourceInfo.TotalSize)),
					result.SourceInfo.TableCount,
					result.SourceInfo.ViewCount))
				fmt.Println()
				return result, nil
			}

			if !prompt.Confirm("Create it?") {
				result.Aborted = true
				return result, nil
			}

			// Create the database
			if err := CreateDatabase(cfg.Destination); err != nil {
				ui.Error(fmt.Sprintf("Failed to create database: %s", err))
				result.Passed = false
				return result, nil
			}
			ui.Success(fmt.Sprintf("Created database %q", cfg.Destination.Database))

			// Try connecting again
			destConnResult, err = Connect(cfg.Destination)
			if err != nil {
				result.Checks = append(result.Checks, CheckResult{
					Name:    "Destination connection",
					Passed:  false,
					Message: err.Error(),
				})
				ui.Error(fmt.Sprintf("Destination connection failed after creating database: %s", err))
				result.Passed = false
				return result, nil
			}
		} else {
			result.Checks = append(result.Checks, CheckResult{
				Name:    "Destination connection",
				Passed:  false,
				Message: err.Error(),
			})
			ui.Error(fmt.Sprintf("Destination connection failed: %s", err))
			result.Passed = false
			return result, nil
		}
	}
	result.DestConn = destConnResult.DB
	result.Checks = append(result.Checks, CheckResult{
		Name:    "Destination connection",
		Passed:  true,
		Message: fmt.Sprintf("Connected to %s://%s@%s:%s/%s", cfg.Destination.Engine, cfg.Destination.User, cfg.Destination.Host, cfg.Destination.Port, cfg.Destination.Database),
	})
	destSSLInfo := ""
	if destConnResult.SSLMode != "" && destConnResult.SSLMode != "require" {
		destSSLInfo = fmt.Sprintf(" (SSL: %s)", destConnResult.SSLMode)
	}
	ui.Success(fmt.Sprintf("Destination connection successful%s", destSSLInfo))

	// 3. Check engine matching
	if cfg.Source.Engine != cfg.Destination.Engine {
		result.Checks = append(result.Checks, CheckResult{
			Name:    "Engine matching",
			Passed:  false,
			Message: fmt.Sprintf("Source engine (%s) does not match destination (%s)", cfg.Source.Engine, cfg.Destination.Engine),
		})
		ui.Error(fmt.Sprintf("Engine mismatch: %s -> %s", cfg.Source.Engine, cfg.Destination.Engine))
		result.Passed = false
		return result, nil
	}
	ui.Success(fmt.Sprintf("Database engines match (%s -> %s)", cfg.Source.Engine, cfg.Destination.Engine))

	// 4. Get destination database info and check versions
	destInfo, err := getDatabaseInfo(result.DestConn, cfg.Destination.Engine, cfg.Destination.Database)
	if err != nil {
		ui.Error(fmt.Sprintf("Failed to get destination database info: %s", err))
		result.Passed = false
		return result, nil
	}
	result.DestInfo = destInfo

	// Version check
	if result.SourceInfo.MajorVersion != destInfo.MajorVersion {
		ui.Warning(fmt.Sprintf("Version mismatch: %s -> %s", result.SourceInfo.Version, destInfo.Version))
		ui.Info("         This tool cannot detect breaking changes between major versions.")
		ui.Info("         Migration will be attempted, but please verify the result.")
		if !prompt.Confirm("Continue anyway?") {
			result.Aborted = true
			return result, nil
		}
		result.Checks = append(result.Checks, CheckResult{
			Name:    "Version check",
			Passed:  true,
			Warning: true,
			Message: fmt.Sprintf("Major version mismatch: %s -> %s (user confirmed)", result.SourceInfo.Version, destInfo.Version),
		})
	} else {
		ui.Success(fmt.Sprintf("Versions: %s -> %s", result.SourceInfo.Version, destInfo.Version))
		result.Checks = append(result.Checks, CheckResult{
			Name:    "Version check",
			Passed:  true,
			Message: fmt.Sprintf("%s -> %s", result.SourceInfo.Version, destInfo.Version),
		})
	}

	// 5. Check if destination is empty
	if destInfo.TableCount > 0 {
		ui.Warning(fmt.Sprintf("Destination database is not empty (%d tables)", destInfo.TableCount))
		if !prompt.Confirm("Drop all objects in destination and continue?") {
			result.Aborted = true
			return result, nil
		}
		// Wipe destination
		if !cfg.DryRun {
			if err := wipeDatabase(result.DestConn, cfg.Destination.Engine); err != nil {
				ui.Error(fmt.Sprintf("Failed to wipe destination: %s", err))
				result.Passed = false
				return result, nil
			}
			ui.Success("Destination database wiped")
		} else {
			ui.DryRun("Would wipe destination database")
		}
	} else {
		ui.Success("Destination database is empty")
	}

	// 6. Show source database summary
	ui.Success(fmt.Sprintf("Source database size: %s (%d tables, %d views)",
		ui.FormatBytes(float64(result.SourceInfo.TotalSize)),
		result.SourceInfo.TableCount,
		result.SourceInfo.ViewCount))

	fmt.Println()

	return result, nil
}

// getDatabaseInfo retrieves database metadata
func getDatabaseInfo(db *sql.DB, engine, database string) (DatabaseInfo, error) {
	info := DatabaseInfo{}

	switch engine {
	case "mysql":
		// Get version
		var version string
		if err := db.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
			return info, fmt.Errorf("failed to get version: %w", err)
		}
		info.Version = version
		info.MajorVersion = extractMajorVersion(version)

		// Get table count and size
		var tableCount int
		var totalSize sql.NullFloat64
		err := db.QueryRow(`
			SELECT COUNT(*), COALESCE(SUM(data_length + index_length), 0)
			FROM information_schema.tables
			WHERE table_schema = ? AND table_type = 'BASE TABLE'
		`, database).Scan(&tableCount, &totalSize)
		if err != nil {
			return info, fmt.Errorf("failed to get table info: %w", err)
		}
		info.TableCount = tableCount
		info.TotalSize = int64(totalSize.Float64)

		// Get view count
		var viewCount int
		err = db.QueryRow(`
			SELECT COUNT(*)
			FROM information_schema.views
			WHERE table_schema = ?
		`, database).Scan(&viewCount)
		if err != nil {
			return info, fmt.Errorf("failed to get view count: %w", err)
		}
		info.ViewCount = viewCount

		// Get table names
		rows, err := db.Query(`
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = ? AND table_type = 'BASE TABLE'
			ORDER BY table_name
		`, database)
		if err != nil {
			return info, fmt.Errorf("failed to get table names: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return info, err
			}
			info.Tables = append(info.Tables, name)
		}

	case "pgsql":
		// Get version
		var version string
		if err := db.QueryRow("SELECT version()").Scan(&version); err != nil {
			return info, fmt.Errorf("failed to get version: %w", err)
		}
		info.Version = version
		info.MajorVersion = extractMajorVersion(version)

		// Get table count
		var tableCount int
		err := db.QueryRow(`
			SELECT COUNT(*)
			FROM information_schema.tables
			WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
		`).Scan(&tableCount)
		if err != nil {
			return info, fmt.Errorf("failed to get table count: %w", err)
		}
		info.TableCount = tableCount

		// Get database size
		var totalSize int64
		err = db.QueryRow("SELECT pg_database_size(current_database())").Scan(&totalSize)
		if err != nil {
			return info, fmt.Errorf("failed to get database size: %w", err)
		}
		info.TotalSize = totalSize

		// Get view count
		var viewCount int
		err = db.QueryRow(`
			SELECT COUNT(*)
			FROM information_schema.views
			WHERE table_schema = 'public'
		`).Scan(&viewCount)
		if err != nil {
			return info, fmt.Errorf("failed to get view count: %w", err)
		}
		info.ViewCount = viewCount

		// Get table names
		rows, err := db.Query(`
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
			ORDER BY table_name
		`)
		if err != nil {
			return info, fmt.Errorf("failed to get table names: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return info, err
			}
			info.Tables = append(info.Tables, name)
		}
	}

	return info, nil
}

// extractMajorVersion extracts the major version number from a version string
func extractMajorVersion(version string) int {
	// Match patterns like "8.0.35", "PostgreSQL 16.2", "5.7.44-log"
	re := regexp.MustCompile(`(\d+)\.(\d+)`)
	matches := re.FindStringSubmatch(version)
	if len(matches) >= 2 {
		major, _ := strconv.Atoi(matches[1])
		return major
	}
	return 0
}

// wipeDatabase drops all tables, views, and other objects from the database
func wipeDatabase(db *sql.DB, engine string) error {
	switch engine {
	case "mysql":
		// Disable foreign key checks
		if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
			return err
		}

		// Get all tables
		rows, err := db.Query(`
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
		`)
		if err != nil {
			return err
		}
		var tables []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return err
			}
			tables = append(tables, name)
		}
		rows.Close()

		// Drop tables
		for _, table := range tables {
			if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table)); err != nil {
				return err
			}
		}

		// Get all views
		rows, err = db.Query(`
			SELECT table_name
			FROM information_schema.views
			WHERE table_schema = DATABASE()
		`)
		if err != nil {
			return err
		}
		var views []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return err
			}
			views = append(views, name)
		}
		rows.Close()

		// Drop views
		for _, view := range views {
			if _, err := db.Exec(fmt.Sprintf("DROP VIEW IF EXISTS `%s`", view)); err != nil {
				return err
			}
		}

		// Re-enable foreign key checks
		if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 1"); err != nil {
			return err
		}

	case "pgsql":
		// Drop all tables with CASCADE
		rows, err := db.Query(`
			SELECT tablename
			FROM pg_tables
			WHERE schemaname = 'public'
		`)
		if err != nil {
			return err
		}
		var tables []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return err
			}
			tables = append(tables, name)
		}
		rows.Close()

		for _, table := range tables {
			if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", quoteIdentifier(table))); err != nil {
				return err
			}
		}

		// Drop all views
		rows, err = db.Query(`
			SELECT viewname
			FROM pg_views
			WHERE schemaname = 'public'
		`)
		if err != nil {
			return err
		}
		var views []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return err
			}
			views = append(views, name)
		}
		rows.Close()

		for _, view := range views {
			if _, err := db.Exec(fmt.Sprintf("DROP VIEW IF EXISTS %s CASCADE", quoteIdentifier(view))); err != nil {
				return err
			}
		}

		// Drop all sequences
		rows, err = db.Query(`
			SELECT sequencename
			FROM pg_sequences
			WHERE schemaname = 'public'
		`)
		if err != nil {
			return err
		}
		var sequences []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return err
			}
			sequences = append(sequences, name)
		}
		rows.Close()

		for _, seq := range sequences {
			if _, err := db.Exec(fmt.Sprintf("DROP SEQUENCE IF EXISTS %s CASCADE", quoteIdentifier(seq))); err != nil {
				return err
			}
		}
	}

	return nil
}

// quoteIdentifier quotes a PostgreSQL identifier
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
