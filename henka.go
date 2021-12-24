package henka

import (
	"fmt"
	"sort"
	"time"
)

type Direction uint

const (
	Down Direction = 0
	Up   Direction = 1
)

// ---

const VersionBits = 64

type Version uint64

type Migration struct {
	Version Version
	Name    string
}

// ---

type MigrationStatus uint

const (
	MigrationPending MigrationStatus = iota
	MigrationApplied
	MigrationMissing
)

// ---

type MigrationDescription struct {
	Migration
	CanUndo bool
}

type MigrationState struct {
	MigrationDescription
	Status    MigrationStatus
	AppliedAt time.Time
}

// ---

type Henka interface {
	Validate() (*ValidationResult, error)
	Upgrade(maxVersion Version) error
	Downgrade(toVersion Version) error
}

type ValidationResult struct {
	Migrations   []MigrationState
	AppliedCount uint
	PendingCount uint
	MissingCount uint
}

// ---

type henkaImpl struct {
	source Source
	driver Driver
}

// ---

func New(source Source, driver Driver) Henka {
	return &henkaImpl{
		source: source,
		driver: driver,
	}
}

// ---

func (m *henkaImpl) Validate() (*ValidationResult, error) {
	availableMigrations, err := m.source.GetAvailableMigrations()
	if err != nil {
		return nil, fmt.Errorf("failed to get the list of available migrations: %w", err)
	}

	appliedMigrations, err := m.loadSortedMigrationsFromDB()
	if err != nil {
		return nil, fmt.Errorf("failed to get the list of applied migrations: %w", err)
	}

	result := ValidationResult{
		Migrations: make([]MigrationState, 0, len(*availableMigrations)),
	}
	for _, availableMigration := range *availableMigrations {
		entry, ok := (*appliedMigrations)[availableMigration.Version]

		var status MigrationStatus
		if ok {
			status = MigrationApplied
			result.AppliedCount++
		} else {
			status = MigrationPending
			result.PendingCount++
		}

		result.Migrations = append(result.Migrations, MigrationState{
			MigrationDescription: availableMigration,
			Status:               status,
			AppliedAt:            entry.AppliedAt,
		})
	}

	for _, applied := range *appliedMigrations {
		found := false

		for _, available := range *availableMigrations {
			if applied.Version == available.Version {
				found = true
				continue
			}
		}

		if !found {
			result.Migrations = append(result.Migrations, MigrationState{
				MigrationDescription: applied.MigrationDescription,
				Status:               MigrationMissing,
				AppliedAt:            applied.AppliedAt,
			})
			result.MissingCount++
		}
	}

	sort.Slice(result.Migrations, func(i, j int) bool {
		return result.Migrations[i].Version < result.Migrations[j].Version
	})

	return &result, nil
}

func (m *henkaImpl) Upgrade(maxVersion Version) error {
	return nil
}

func (m *henkaImpl) Downgrade(toVersion Version) error {
	return nil
}

func (m *henkaImpl) loadSortedMigrationsFromDB() (*map[Version]MigrationState, error) {
	migrations, err := m.driver.ListAppliedMigrations()
	if err != nil {
		return nil, fmt.Errorf("failed to load migrations from db: %w", err)
	}

	result := make(map[Version]MigrationState, len(*migrations))
	for _, m := range *migrations {
		result[m.Version] = m
	}

	return &result, nil
}
