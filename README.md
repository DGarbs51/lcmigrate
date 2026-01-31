# Laravel Cloud Migrate

[![Tests](https://github.com/DGarbs51/lcmigrate/actions/workflows/test.yml/badge.svg)](https://github.com/DGarbs51/lcmigrate/actions/workflows/test.yml)

CLI tool for migrating MySQL and PostgreSQL databases to Laravel Cloud.

## Installation

```bash
go build -ldflags="-s -w" -o lcmigrate .
```

## Commands

### Analyze

Inspect a database and display statistics (table counts, sizes, indexes, foreign keys):

```bash
./lcmigrate analyze
```

### Migrate

Migrate a database from source to destination:

```bash
./lcmigrate migrate
```

Preview what would be migrated without making changes:

```bash
./lcmigrate migrate --dry-run
```

The migration process:
1. Prompts for source and destination credentials
2. Runs pre-flight validation (connections, version compatibility, empty destination check)
3. Migrates schema (tables without indexes/FKs)
4. Transfers data in batches
5. Creates indexes and foreign keys
6. Creates views
7. Migrates sequences (PostgreSQL only)
8. Verifies row counts

## Configuration

Create a `.env` file to set default connection values:

```env
# Source database (used by both analyze and migrate)
DB_ENGINE=mysql          # or pgsql
DB_HOST=localhost
DB_PORT=3306
DB_DATABASE=myapp
DB_USER=root
DB_PASSWORD=secret

# For migrate command, you can also use prefixed variables:
SOURCE_DB_HOST=source.example.com
DESTINATION_DB_HOST=destination.example.com
DESTINATION_DB_USER=admin
DESTINATION_DB_PASSWORD=secret
```

The tool will prompt for any missing values interactively.

## Supported Databases

- MySQL / MariaDB
- PostgreSQL
