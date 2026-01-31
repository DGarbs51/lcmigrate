package data

import "github.com/DGarbs51/lcmigrate/internal/dialect"

// MySQLTransferer handles data transfer for MySQL databases
type MySQLTransferer struct {
	BaseTransferer
}

// NewMySQLTransferer creates a new MySQL data transferer
func NewMySQLTransferer() *MySQLTransferer {
	return &MySQLTransferer{
		BaseTransferer: BaseTransferer{
			Dialect: &dialect.MySQLDialect{},
		},
	}
}
