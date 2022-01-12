package henka

import (
	"fmt"
	"sort"
	"time"

	"github.com/root-talis/henka/driver"
	"github.com/root-talis/henka/migration"
	source2 "github.com/root-talis/henka/source"
)

// ---

type Henka interface {
	Validate() (*ValidationResult, error)
	Upgrade(maxVersion migration.Version) error
	Downgrade(toVersion migration.Version) error
}

type ValidationResult struct {
	Migrations   []migration.State
	AppliedCount uint
	PendingCount uint
	MissingCount uint
}

// ---

type henkaImpl struct {
	source source2.Source
	driver driver.Driver
}

// ---

func New(source source2.Source, driver driver.Driver) Henka {
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
		Migrations: make([]migration.State, 0, len(*availableMigrations)),
	}
	for _, availableMigration := range *availableMigrations {
		entry, ok := (*appliedMigrations)[availableMigration.Version]

		var status migration.Status
		if ok {
			status = entry.Status
		} else {
			status = migration.Pending
		}

		if status == migration.Pending {
			result.PendingCount++
		} else {
			result.AppliedCount++
		}

		result.Migrations = append(result.Migrations, migration.State{
			Description: availableMigration,
			Status:      status,
			AppliedAt:   entry.AppliedAt,
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

		applied.Description.CanUndo = false

		if !found {
			result.Migrations = append(result.Migrations, migration.State{
				Description: applied.Description,
				Status:      migration.Missing,
				AppliedAt:   applied.AppliedAt,
			})
			result.MissingCount++
		}
	}

	sort.Slice(result.Migrations, func(i, j int) bool {
		return result.Migrations[i].Version < result.Migrations[j].Version
	})

	return &result, nil
}

func (m *henkaImpl) Upgrade(maxVersion migration.Version) error {
	return nil
}

func (m *henkaImpl) Downgrade(toVersion migration.Version) error {
	return nil
}

func (m *henkaImpl) loadSortedMigrationsFromDB() (*map[migration.Version]migration.State, error) {
	migrations, err := m.driver.ListMigrationsLog()
	if err != nil {
		return nil, fmt.Errorf("failed to load migrations from db: %w", err)
	}

	result := make(map[migration.Version]migration.State, len(*migrations))
	for _, mig := range *migrations {
		var status migration.Status
		var appliedAt time.Time

		switch mig.Direction {
		case migration.Up:
			status = migration.Applied
			appliedAt = mig.AppliedAt
		case migration.Down:
			status = migration.Pending
		}

		result[mig.Version] = migration.State{
			Description: migration.Description{
				Migration: mig.Migration,
				CanUndo:   false,
			},
			Status:    status,
			AppliedAt: appliedAt,
		}
	}

	return &result, nil
}
