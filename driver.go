package mig

type Driver interface {
	ListAppliedMigrations() (*[]MigrationState, error)
}
