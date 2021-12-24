package henka

type Driver interface {
	ListAppliedMigrations() (*[]MigrationState, error)
}
