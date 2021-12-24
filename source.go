package mig

import (
	"errors"
	"io"
)

type Source interface {
	GetAvailableMigrations() (*[]MigrationDescription, error)
	ReadMigration(migration Migration, direction Direction) (io.Reader, error)
}

var (
	ErrMigrationDuplicated = errors.New("migration version already exists with different name")
)
