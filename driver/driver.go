package driver

import (
	"github.com/root-talis/henka/migration"
)

type Driver interface {
	ListAppliedMigrations() (*[]migration.State, error)
}
