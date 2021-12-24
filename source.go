package mig

import (
	"io"
)

type Source interface {
	GetAvailableMigrations() (*[]MigrationDescription, error)
	ReadMigration(version Version, direction Direction) (io.Reader, error)
}
