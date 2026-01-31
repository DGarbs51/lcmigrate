# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

lcmigrate is a Go CLI tool for migrating MySQL or PostgreSQL databases between servers. It provides two main commands: `analyze` (display database analytics) and `migrate` (full database migration with schema, data, indexes, views, and sequences).

## Common Commands

```bash
# Build the project
go build -v ./...

# Run tests
go test -v ./...

# Build optimized binary
go build -ldflags="-s -w" -o lcmigrate .

# Run the CLI
./lcmigrate analyze          # Analyze a single database
./lcmigrate migrate          # Migrate source to destination
./lcmigrate migrate --dry-run # Preview migration without changes
```

## Architecture

### Entry Points
- **main.go** - Entry point, calls `cmd.Execute()`
- **cmd/root.go** - Cobra CLI with `analyze` and `migrate` commands

### Database Analysis (db/ package)
- **db/connection.go** - Connection config, interactive prompts for analyze command
- **db/mysql.go** / **db/postgres.go** - Engine-specific analysis queries

### Migration Pipeline (internal/ packages)
The migration uses a staged pipeline orchestrated by `internal/migrator`:

1. **internal/config** - `MigrationConfig` holds source/destination configs
2. **internal/prompt** - Interactive credential prompts for migration
3. **internal/preflight** - Pre-flight checks: connections, version matching, destination wipe
4. **internal/schema** - `Extractor`/`Applier` interfaces for extracting and applying schemas
5. **internal/data** - `Transferer` interface for batch data transfer
6. **internal/ui** - Progress bars and formatted output
7. **internal/migrator** - Orchestrates the 6-stage migration workflow

### Engine-Specific Implementations
Both `internal/schema` and `internal/data` have MySQL and PostgreSQL implementations (`mysql.go`/`postgres.go`) that implement their respective interfaces.

## Key Dependencies

- `github.com/spf13/cobra` - CLI framework
- `github.com/go-sql-driver/mysql` - MySQL driver
- `github.com/lib/pq` - PostgreSQL driver
- `github.com/joho/godotenv` - .env file loading
- `github.com/fatih/color` - Terminal colors

## Environment Variables

The tool auto-loads `.env` files. For the `analyze` command:
- `DB_ENGINE` or `DB_CONNECTION` - mysql/pgsql
- `DB_HOST`, `DB_PORT`, `DB_DATABASE` (or `DB_NAME`)
- `DB_USER` (or `DB_USERNAME`), `DB_PASSWORD`

For the `migrate` command, use prefixed variables:
- `SOURCE_DB_*` - Source database (falls back to unprefixed `DB_*`)
- `DESTINATION_DB_*` - Destination database (no fallback)
