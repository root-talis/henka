package source

import (
	"errors"
	"io"

	"github.com/root-talis/henka/migration"
)

type Source interface {
	GetAvailableMigrations() (*[]migration.Description, error)
	ReadMigration(migration migration.Migration, direction migration.Direction) (io.Reader, error)
}

var (
	ErrMigrationDuplicated = errors.New("migration version already exists with different name")
)
