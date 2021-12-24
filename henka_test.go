package henka_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/root-talis/henka"
)

// -- testing double for source ----------

type sourceGetAvailableMigrationsResult struct {
	descr []henka.MigrationDescription
	err   error
}

type sourceMock struct {
	availableMigrations sourceGetAvailableMigrationsResult
}

func (m *sourceMock) GetAvailableMigrations() (*[]henka.MigrationDescription, error) {
	return &m.availableMigrations.descr, m.availableMigrations.err
}

func (m *sourceMock) ReadMigration(migration henka.Migration, direction henka.Direction) (io.Reader, error) {
	return nil, nil
}

// -- testing double for driver ----------

type driverListAppliedMigrationsResult struct {
	state []henka.MigrationState
	err   error
}

type driverMock struct {
	appliedMigrations driverListAppliedMigrationsResult
}

func (m *driverMock) ListAppliedMigrations() (*[]henka.MigrationState, error) {
	return &m.appliedMigrations.state, m.appliedMigrations.err
}

//
// -- Tests for Henka.Validate() ------------
//

var migrations = []henka.MigrationDescription{ // nolint:gochecknoglobals
	{Migration: henka.Migration{Version: 20210124131258, Name: "initial_structure"}, CanUndo: false},
	{Migration: henka.Migration{Version: 20210124132201, Name: "indexes"}, CanUndo: true},
	{Migration: henka.Migration{Version: 20210608080143, Name: "sessions_table"}, CanUndo: true},
	{Migration: henka.Migration{Version: 20210608080148, Name: "sessions_table_indexes"}, CanUndo: true},
}

var ErrAny = errors.New("test error")

var validateTestsTable = []struct { // nolint:gochecknoglobals
	name                string
	availableMigrations sourceGetAvailableMigrationsResult
	appliedMigrations   driverListAppliedMigrationsResult

	expectedResult henka.ValidationResult
	expectError    bool
}{
	// -- success cases: ---
	/* 0 */ {
		name: "test 0: should spot all pending migrations (0)",
		availableMigrations: sourceGetAvailableMigrationsResult{
			descr: []henka.MigrationDescription{
				// empty
			}, err: nil,
		},
		appliedMigrations: driverListAppliedMigrationsResult{
			// empty
		},
		expectedResult: henka.ValidationResult{
			Migrations: []henka.MigrationState{
				// empty
			},
			PendingCount: 0,
		},
	},
	/* 1 */ {
		name: "test 1: should spot all pending migrations (1)",
		availableMigrations: sourceGetAvailableMigrationsResult{
			descr: []henka.MigrationDescription{migrations[1]}, err: nil,
		},
		appliedMigrations: driverListAppliedMigrationsResult{
			// empty
		},
		expectedResult: henka.ValidationResult{
			Migrations: []henka.MigrationState{
				{MigrationDescription: migrations[1], Status: henka.MigrationPending},
			},
			PendingCount: 1,
		},
	},
	/* 2 */ {
		name: "test 2: should spot all pending migrations (2)",
		availableMigrations: sourceGetAvailableMigrationsResult{
			descr: []henka.MigrationDescription{migrations[0], migrations[1]}, err: nil,
		},
		appliedMigrations: driverListAppliedMigrationsResult{
			// empty
		},
		expectedResult: henka.ValidationResult{
			Migrations: []henka.MigrationState{
				{MigrationDescription: migrations[0], Status: henka.MigrationPending},
				{MigrationDescription: migrations[1], Status: henka.MigrationPending},
			},
			PendingCount: 2,
		},
	},
	/* 3 */ {
		name: "test 3: should spot all applied migrations (1)",
		availableMigrations: sourceGetAvailableMigrationsResult{
			descr: []henka.MigrationDescription{migrations[0]}, err: nil,
		},
		appliedMigrations: driverListAppliedMigrationsResult{
			state: []henka.MigrationState{
				{MigrationDescription: migrations[0], Status: henka.MigrationApplied, AppliedAt: time.Unix(12345, 0)},
			},
		},
		expectedResult: henka.ValidationResult{
			Migrations: []henka.MigrationState{
				{MigrationDescription: migrations[0], Status: henka.MigrationApplied, AppliedAt: time.Unix(12345, 0)},
			},
			AppliedCount: 1,
		},
	},
	/* 4 */ {
		name: "test 4: should spot all applied migrations (2)",
		availableMigrations: sourceGetAvailableMigrationsResult{
			descr: []henka.MigrationDescription{migrations[1], migrations[2]}, err: nil,
		},
		appliedMigrations: driverListAppliedMigrationsResult{
			state: []henka.MigrationState{
				{MigrationDescription: migrations[1], AppliedAt: time.Unix(12345, 0)},
				{MigrationDescription: migrations[2], AppliedAt: time.Unix(12346, 0)},
			},
		},
		expectedResult: henka.ValidationResult{
			Migrations: []henka.MigrationState{
				{MigrationDescription: migrations[1], Status: henka.MigrationApplied, AppliedAt: time.Unix(12345, 0)},
				{MigrationDescription: migrations[2], Status: henka.MigrationApplied, AppliedAt: time.Unix(12346, 0)},
			},
			AppliedCount: 2,
		},
	},
	/* 5 */ {
		name: "test 5: should spot all missing migrations (1)",
		availableMigrations: sourceGetAvailableMigrationsResult{
			descr: []henka.MigrationDescription{}, err: nil,
		},
		appliedMigrations: driverListAppliedMigrationsResult{
			state: []henka.MigrationState{
				{MigrationDescription: migrations[1], AppliedAt: time.Unix(12345, 0)},
			},
		},
		expectedResult: henka.ValidationResult{
			Migrations: []henka.MigrationState{
				{MigrationDescription: migrations[1], Status: henka.MigrationMissing, AppliedAt: time.Unix(12345, 0)},
			},
			MissingCount: 1,
		},
	},
	/* 6 */ {
		name: "test 6: should spot all missing migrations (2)",
		availableMigrations: sourceGetAvailableMigrationsResult{
			descr: []henka.MigrationDescription{}, err: nil,
		},
		appliedMigrations: driverListAppliedMigrationsResult{
			state: []henka.MigrationState{
				{MigrationDescription: migrations[0], AppliedAt: time.Unix(12345, 0)},
				{MigrationDescription: migrations[2], AppliedAt: time.Unix(12346, 0)},
			},
		},
		expectedResult: henka.ValidationResult{
			Migrations: []henka.MigrationState{
				{MigrationDescription: migrations[0], Status: henka.MigrationMissing, AppliedAt: time.Unix(12345, 0)},
				{MigrationDescription: migrations[2], Status: henka.MigrationMissing, AppliedAt: time.Unix(12346, 0)},
			},
			MissingCount: 2,
		},
	},
	/* 7 */ {
		name: "test 7: should correctly sort missing migrations",
		availableMigrations: sourceGetAvailableMigrationsResult{
			descr: []henka.MigrationDescription{migrations[0], migrations[2]}, err: nil,
		},
		appliedMigrations: driverListAppliedMigrationsResult{
			state: []henka.MigrationState{
				{MigrationDescription: migrations[0], AppliedAt: time.Unix(12345, 0)},
				{MigrationDescription: migrations[1], AppliedAt: time.Unix(12346, 0)},
				{MigrationDescription: migrations[2], AppliedAt: time.Unix(12347, 0)},
			},
		},
		expectedResult: henka.ValidationResult{
			Migrations: []henka.MigrationState{
				{MigrationDescription: migrations[0], Status: henka.MigrationApplied, AppliedAt: time.Unix(12345, 0)},
				{MigrationDescription: migrations[1], Status: henka.MigrationMissing, AppliedAt: time.Unix(12346, 0)},
				{MigrationDescription: migrations[2], Status: henka.MigrationApplied, AppliedAt: time.Unix(12347, 0)},
			},
			AppliedCount: 2,
			MissingCount: 1,
		},
	},
	/* 8 */ {
		name: "test 8: should correctly evaluate complex state",
		availableMigrations: sourceGetAvailableMigrationsResult{
			descr: []henka.MigrationDescription{migrations[0], migrations[1], migrations[3]}, err: nil,
		},
		appliedMigrations: driverListAppliedMigrationsResult{
			state: []henka.MigrationState{
				{MigrationDescription: migrations[0], AppliedAt: time.Unix(12345, 0)},
				{MigrationDescription: migrations[2], AppliedAt: time.Unix(12346, 0)},
			},
		},
		expectedResult: henka.ValidationResult{
			Migrations: []henka.MigrationState{
				{MigrationDescription: migrations[0], Status: henka.MigrationApplied, AppliedAt: time.Unix(12345, 0)},
				{MigrationDescription: migrations[1], Status: henka.MigrationPending},
				{MigrationDescription: migrations[2], Status: henka.MigrationMissing, AppliedAt: time.Unix(12346, 0)},
				{MigrationDescription: migrations[3], Status: henka.MigrationPending},
			},
			PendingCount: 2,
			AppliedCount: 1,
			MissingCount: 1,
		},
	},

	// -- error cases: -----
	/* 9 */ {
		name: "test 9: should return error if dst.GetAvailableMigrations fails",
		availableMigrations: sourceGetAvailableMigrationsResult{
			descr: nil, err: ErrAny,
		},
		appliedMigrations: driverListAppliedMigrationsResult{
			state: []henka.MigrationState{
				{MigrationDescription: migrations[0], AppliedAt: time.Unix(12345, 0)},
				{MigrationDescription: migrations[2], AppliedAt: time.Unix(12346, 0)},
			},
		},
		expectError: true,
	},
	/* 10 */ {
		name: "test 10: should return error if source.ListAppliedMigrations fails",
		availableMigrations: sourceGetAvailableMigrationsResult{
			descr: []henka.MigrationDescription{migrations[0], migrations[1], migrations[3]}, err: nil,
		},
		appliedMigrations: driverListAppliedMigrationsResult{
			err: ErrAny,
		},
		expectError: true,
	},
}

func TestValidate(t *testing.T) {
	t.Parallel()
	t.Logf("Should correctly evaluate current database state.")

	for _, test := range validateTestsTable {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			src := sourceMock{availableMigrations: test.availableMigrations}
			drv := driverMock{appliedMigrations: test.appliedMigrations}

			migrator := henka.New(&src, &drv)
			result, err := migrator.Validate()

			if test.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, *result, test.expectedResult)
			}
		})
	}
}
