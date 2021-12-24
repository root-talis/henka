package mig_test

import (
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"

	"github.com/root-talis/mig"
)

var getAvailableMigrationsTestTable = []struct { // nolint:gochecknoglobals
	name                    string
	expectErrorWhenCreating bool
	expectErrorWhenCalling  bool
	directory               string
	fs                      fstest.MapFS
	expectedMigrations      []mig.MigrationDescription
}{
	// -- success tests ------
	/* s0 */ {
		name:      "test s0: should correctly list all migrations (1)",
		directory: "migrations",
		fs: fstest.MapFS{
			"migrations": {
				Mode: fs.ModeDir,
			},
			"migrations/V20211224091800_add_users_table.down.sql": {},
			"migrations/V20211224091800_add_users_table.up.sql":   {},
		},
		expectedMigrations: []mig.MigrationDescription{
			{Migration: mig.Migration{Version: 20211224091800, Name: "add_users_table"}, CanUndo: true},
		},
	},
	/* s1 */ {
		name:      "test s1: should correctly list all migrations (2)",
		directory: "migrations",
		fs: fstest.MapFS{
			"migrations": {
				Mode: fs.ModeDir,
			},
			"migrations/V20211224081255_initial.up.sql":           {},
			"migrations/V20211224091800_add_users_table.down.sql": {},
			"migrations/V20211224091800_add_users_table.up.sql":   {},
		},
		expectedMigrations: []mig.MigrationDescription{
			{Migration: mig.Migration{Version: 20211224081255, Name: "initial"}, CanUndo: false},
			{Migration: mig.Migration{Version: 20211224091800, Name: "add_users_table"}, CanUndo: true},
		},
	},
	/* s2 */ {
		name:      "test s2: should correctly list migrations in an non-standard directory",
		directory: "tmp/.Xs223xxSCa",
		fs: fstest.MapFS{
			"tmp/.Xs223xxSCa": {
				Mode: fs.ModeDir,
			},
			"tmp/.Xs223xxSCa/V20211224081255_initial.up.sql":           {},
			"tmp/.Xs223xxSCa/V20211224091800_add_users_table.down.sql": {},
			"tmp/.Xs223xxSCa/V20211224091800_add_users_table.up.sql":   {},
		},
		expectedMigrations: []mig.MigrationDescription{
			{Migration: mig.Migration{Version: 20211224081255, Name: "initial"}, CanUndo: false},
			{Migration: mig.Migration{Version: 20211224091800, Name: "add_users_table"}, CanUndo: true},
		},
	},
	/* s3 */ {
		name:      "test s3: should skip on bad version format (too short)",
		directory: "migrations",
		fs: fstest.MapFS{
			"migrations": {
				Mode: fs.ModeDir,
			},
			"migrations/V2021122409180_init.up.sql":               {},
			"migrations/V20211224091800_add_users_table.down.sql": {},
			"migrations/V20211224091800_add_users_table.up.sql":   {},
		},
		expectedMigrations: []mig.MigrationDescription{
			{Migration: mig.Migration{Version: 20211224091800, Name: "add_users_table"}, CanUndo: true},
		},
	},
	/* s4 */ {
		name:      "test s4: should skip on bad version format (does not start with a digit)",
		directory: "migrations",
		fs: fstest.MapFS{
			"migrations": {
				Mode: fs.ModeDir,
			},
			"migrations/V_0211224091800_init.up.sql":              {},
			"migrations/V20211224091800_add_users_table.down.sql": {},
			"migrations/V20211224091800_add_users_table.up.sql":   {},
		},
		expectedMigrations: []mig.MigrationDescription{
			{Migration: mig.Migration{Version: 20211224091800, Name: "add_users_table"}, CanUndo: true},
		},
	},
	/* s5 */ {
		name:      "test s5: should skip on bad version format (does not start with a V)",
		directory: "migrations",
		fs: fstest.MapFS{
			"migrations": {
				Mode: fs.ModeDir,
			},
			"migrations/120211224091800_init.up.sql":              {},
			"migrations/V20211224091800_add_users_table.down.sql": {},
			"migrations/V20211224091800_add_users_table.up.sql":   {},
		},
		expectedMigrations: []mig.MigrationDescription{
			{Migration: mig.Migration{Version: 20211224091800, Name: "add_users_table"}, CanUndo: true},
		},
	},
	/* s6 */ {
		name:      "test s6: should skip on bad migration name (no underscore before name)",
		directory: "migrations",
		fs: fstest.MapFS{
			"migrations": {
				Mode: fs.ModeDir,
			},
			"migrations/V20211224091800init.up.sql":               {},
			"migrations/V20211224091800_add_users_table.down.sql": {},
			"migrations/V20211224091800_add_users_table.up.sql":   {},
		},
		expectedMigrations: []mig.MigrationDescription{
			{Migration: mig.Migration{Version: 20211224091800, Name: "add_users_table"}, CanUndo: true},
		},
	},
	/* s7 */ {
		name:      "test s7: should skip on bad migration name (no name)",
		directory: "migrations",
		fs: fstest.MapFS{
			"migrations": {
				Mode: fs.ModeDir,
			},
			"migrations/V20211224091800.up.sql":                   {},
			"migrations/V20211224091800_add_users_table.down.sql": {},
			"migrations/V20211224091800_add_users_table.up.sql":   {},
		},
		expectedMigrations: []mig.MigrationDescription{
			{Migration: mig.Migration{Version: 20211224091800, Name: "add_users_table"}, CanUndo: true},
		},
	},
	/* s8 */ {
		name:      "test s8: should skip on bad migration name (no name but with underscore)",
		directory: "migrations",
		fs: fstest.MapFS{
			"migrations": {
				Mode: fs.ModeDir,
			},
			"migrations/V20211224091800_.up.sql":                  {},
			"migrations/V20211224091800_add_users_table.down.sql": {},
			"migrations/V20211224091800_add_users_table.up.sql":   {},
		},
		expectedMigrations: []mig.MigrationDescription{
			{Migration: mig.Migration{Version: 20211224091800, Name: "add_users_table"}, CanUndo: true},
		},
	},
	/* s9 */ {
		name:      "test s9: should skip on bad migration name (bad suffix)",
		directory: "migrations",
		fs: fstest.MapFS{
			"migrations": {
				Mode: fs.ModeDir,
			},
			"migrations/V20211224091800_init..sql":                {},
			"migrations/V20211224091800_init.sql":                 {},
			"migrations/V20211224091800_init.up":                  {},
			"migrations/V20211224091800_init.":                    {},
			"migrations/V20211224091800_init":                     {},
			"migrations/V20211224091800_add_users_table.down.sql": {},
			"migrations/V20211224091800_add_users_table.up.sql":   {},
		},
		expectedMigrations: []mig.MigrationDescription{
			{Migration: mig.Migration{Version: 20211224091800, Name: "add_users_table"}, CanUndo: true},
		},
	},
	/* s10 */ {
		name:      "test s10: should not care about other directories",
		directory: "migrations",
		fs: fstest.MapFS{
			"migrations": {
				Mode: fs.ModeDir,
			},
			"V20211224091100_init.up.sql":                         {},
			"migrations/subdirectory/V20211224091100_init.up.sql": {},
			"sibling/V20211224091100_init.up.sql":                 {},
			"migrations/V20211224091800_add_users_table.down.sql": {},
			"migrations/V20211224091800_add_users_table.up.sql":   {},
		},
		expectedMigrations: []mig.MigrationDescription{
			{Migration: mig.Migration{Version: 20211224091800, Name: "add_users_table"}, CanUndo: true},
		},
	},
	/* s11 */ {
		name:      "test s11: should skip directories with matching name",
		directory: "migrations",
		fs: fstest.MapFS{
			"migrations": {
				Mode: fs.ModeDir,
			},
			"migrations/V20211224091700_init.up.sql": {
				Mode: fs.ModeDir,
			},
			"migrations/V20211224091800_add_users_table.down.sql": {},
			"migrations/V20211224091800_add_users_table.up.sql":   {},
		},
		expectedMigrations: []mig.MigrationDescription{
			{Migration: mig.Migration{Version: 20211224091800, Name: "add_users_table"}, CanUndo: true},
		},
	},

	// -- error tests --------
	/* e0 */ {
		name:      "test e0: should fail when directory does not exist",
		directory: "mig",
		fs: fstest.MapFS{
			"migrations": {
				Mode: fs.ModeDir,
			},
			"migrations/V20211224081255_initial.up.sql": {},
		},
		expectErrorWhenCreating: true,
	},
	/* e1 */ {
		name:      "test e1: should fail on duplicate migration version",
		directory: "migrations",
		fs: fstest.MapFS{
			"migrations": {
				Mode: fs.ModeDir,
			},
			"migrations/V20211224091800_add_users_table.down.sql":   {},
			"migrations/V20211224091800_add_users_table.up.sql":     {},
			"migrations/V20211224091800_add_users_table_2.down.sql": {},
		},
		expectErrorWhenCalling: true,
	},
	/* e2 */ {
		name:      "test e2: should fail when directory is a file",
		directory: "migrations",
		fs: fstest.MapFS{
			"migrations": {},
		},
		expectErrorWhenCreating: true,
	},
	/* e3 */ {
		name:      "test e3: should fail when directory is a device",
		directory: "migrations",
		fs: fstest.MapFS{
			"migrations": {
				Mode: fs.ModeDevice,
			},
		},
		expectErrorWhenCreating: true,
	},
}

func TestGetAvailableMigrations(t *testing.T) {
	t.Parallel()
	t.Logf("Should correctly test fetching of available migrations from a directory.")

	for _, test := range getAvailableMigrationsTestTable {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			src, err := mig.NewFilesSource(test.fs, test.directory)

			if test.expectErrorWhenCreating {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			migrations, err := src.GetAvailableMigrations()

			if test.expectErrorWhenCalling {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			if assert.NotNil(t, migrations) {
				assert.Equal(t, test.expectedMigrations, *migrations)
			}
		})
	}
}
