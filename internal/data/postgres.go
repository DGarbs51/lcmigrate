package data

import "github.com/DGarbs51/lcmigrate/internal/dialect"

// PostgresTransferer handles data transfer for PostgreSQL databases
type PostgresTransferer struct {
	BaseTransferer
}

// NewPostgresTransferer creates a new PostgreSQL data transferer
func NewPostgresTransferer() *PostgresTransferer {
	return &PostgresTransferer{
		BaseTransferer: BaseTransferer{
			Dialect: &dialect.PostgresDialect{},
		},
	}
}
