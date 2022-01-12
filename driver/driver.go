package driver

import (
	"errors"

	"github.com/root-talis/henka/migration"
)

type Driver interface {
	ListMigrationsLog() (*[]migration.Log, error)
}

var ErrInvalidLogTable = errors.New("an error has occurred when reading log table")
