# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

lcmigrate is a Go CLI tool for connecting to MySQL or PostgreSQL databases and displaying comprehensive analytics including table counts, sizes, schema details, indexes, foreign keys, and connection statistics.

## Common Commands

```bash
# Build the project
go build -v ./...

# Run tests
go test -v ./...

# Build optimized binary
go build -ldflags="-s -w" -o lcmigrate .

# Run the CLI
./lcmigrate analyze
```

## Architecture

- **main.go** - Entry point, calls `cmd.Execute()`
- **cmd/root.go** - Cobra CLI setup with `analyze` command
- **db/connection.go** - Connection config, interactive prompts, and routing to engine-specific analyzers
- **db/mysql.go** - MySQL-specific analysis queries and formatting
- **db/postgres.go** - PostgreSQL-specific analysis queries and formatting

## Key Dependencies

- `github.com/spf13/cobra` - CLI framework
- `github.com/go-sql-driver/mysql` - MySQL driver
- `github.com/lib/pq` - PostgreSQL driver
- `github.com/joho/godotenv` - .env file loading
- `github.com/fatih/color` - Terminal colors

## Environment Variables

The tool auto-loads `.env` files and accepts these variables as connection defaults:
- `DB_ENGINE` or `DB_CONNECTION` - mysql/pgsql
- `DB_HOST`, `DB_PORT`, `DB_DATABASE` (or `DB_NAME`)
- `DB_USER` (or `DB_USERNAME`), `DB_PASSWORD`
