//nolint:gochecknoglobals
package mysql_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/root-talis/henka/driver/mysql"
	"github.com/root-talis/henka/migration"
)

// RDBMS versions to test against
var versions = []string{
	"mysql:8.0",
	"mysql:5.7",
	"mysql:5.6",

	"mariadb:10.7",
	"mariadb:10.6",
	"mariadb:10.5",
	"mariadb:10.4",
	"mariadb:10.3",
	"mariadb:10.2",
}

// Templates for test tables
var (
	dropDatabase               = "DROP DATABASE testDatabase;"
	initEmptyDatabase          = "CREATE DATABASE testDatabase;"
	initDatabaseWithEmptyTable = initEmptyDatabase +
		"CREATE TABLE testDatabase.migrations_log (" +
		"id             int not null auto_increment, " +
		"version        bigint, " +
		"migration_name varchar(100) null, " +
		"direction      char(1) null, " + // "u" or "d"
		"start_time     datetime default CURRENT_TIMESTAMP not null, " +
		"end_time       datetime null, " +
		"primary key (id)" +
		") default charset utf8;"
	initDatabaseWithBadTableStructure = initEmptyDatabase +
		"CREATE TABLE testDatabase.migrations_log (" +
		"id             int not null auto_increment, " +
		"primary key (id)" +
		") default charset utf8;"

	defaultDriverConfig = mysql.DriverConfig{
		DatabaseName:        "testDatabase",
		MigrationsTableName: "migrations_log",
	}

	insertMigration = "INSERT INTO testDatabase.migrations_log (version, migration_name, direction, start_time, end_time) VALUES "
	migration1Sql   = insertMigration + "(\"20220118115519\", \"createUsersTable\", \"u\", \"2022-01-19 10:00:00\", \"2022-01-19 10:00:01\");"
	migration2Sql   = insertMigration + "(\"20220118115519\", \"createUsersTable\", \"d\", \"2022-01-19 10:02:00\", \"2022-01-19 10:02:01\");"
	migration3Sql   = insertMigration + "(\"20220118115519\", \"createUsersTable\", \"u\", \"2022-01-19 10:03:00\", \"2022-01-19 10:03:01\");"
	migration4Sql   = insertMigration + "(\"20220118120101\", \"createPermissionsTable\", \"u\", \"2022-01-19 10:04:00\", \"2022-01-19 10:04:01\");"

	migration1Parsed = migration.Log{
		Migration: migration.Migration{Version: 20220118115519, Name: "createUsersTable"},
		Direction: migration.Up,
		AppliedAt: time.Date(2022, 1, 19, 10, 0, 0, 0, time.UTC),
	}
	migration2Parsed = migration.Log{
		Migration: migration.Migration{Version: 20220118115519, Name: "createUsersTable"},
		Direction: migration.Down,
		AppliedAt: time.Date(2022, 1, 19, 10, 2, 0, 0, time.UTC),
	}
	migration3Parsed = migration.Log{
		Migration: migration.Migration{Version: 20220118115519, Name: "createUsersTable"},
		Direction: migration.Up,
		AppliedAt: time.Date(2022, 1, 19, 10, 3, 0, 0, time.UTC),
	}
	migration4Parsed = migration.Log{
		Migration: migration.Migration{Version: 20220118120101, Name: "createPermissionsTable"},
		Direction: migration.Up,
		AppliedAt: time.Date(2022, 1, 19, 10, 4, 0, 0, time.UTC),
	}
	migrationsSet1Parsed = []migration.Log{
		migration1Parsed, migration2Parsed, migration3Parsed, migration4Parsed,
	}

	initDatabaseWithMigrationsSet1 = initDatabaseWithEmptyTable +
		migration1Sql +
		migration2Sql +
		migration3Sql +
		migration4Sql
)

type validator = func(*testing.T, *sql.Rows)
type validateStatements = map[string]validator

var doNothing = func(t *testing.T, _ *sql.Rows) {
	t.Helper()
}

// Test table for TestListMigrationsLog
var listMigrationsLogTests = []struct {
	name               string
	expectError        bool
	initialStructure   string
	driverConfig       mysql.DriverConfig
	validateStatements validateStatements
	expectedLog        *[]migration.Log
}{
	/* s0 */ {
		name:             "test s0 - should create new migrations_log table",
		initialStructure: initEmptyDatabase,
		driverConfig:     defaultDriverConfig,
		validateStatements: validateStatements{
			"select 1 from testDatabase.migrations_log": doNothing,
		},
		expectedLog: &[]migration.Log{}, // empty
	},
	/* s1 */ {
		name:             "test s1 - should create new migrations_log table with a custom name",
		initialStructure: initEmptyDatabase,
		driverConfig: mysql.DriverConfig{
			DatabaseName:        "testDatabase",
			MigrationsTableName: "some_strange_custom_migrations_log_table",
		},
		validateStatements: map[string]func(*testing.T, *sql.Rows){
			"select 1 from testDatabase.some_strange_custom_migrations_log_table": doNothing,
		},
		expectedLog: &[]migration.Log{}, // empty
	},
	/* s2 */ {
		name:             "test s2 - should not create another migrations_log table",
		initialStructure: initDatabaseWithEmptyTable,
		driverConfig:     defaultDriverConfig,
		validateStatements: map[string]func(*testing.T, *sql.Rows){
			"select 1 from testDatabase.migrations_log": doNothing,
		},
		expectedLog: &[]migration.Log{}, // empty
	},
	/* s3 */ {
		name:             "test s3 - should return correct log from database",
		initialStructure: initDatabaseWithMigrationsSet1,
		driverConfig:     defaultDriverConfig,
		expectedLog:      &migrationsSet1Parsed,
	},

	/* e0 */ {
		name:             "test e0 - should fail if database doesn't exist",
		initialStructure: initEmptyDatabase,
		expectError:      true,
		driverConfig: mysql.DriverConfig{
			DatabaseName:        "wrongTestDatabase",
			MigrationsTableName: "migrations_log",
		},
	},
	/* e1 */ {
		name:             "test e1 - should fail if migrations_log table has bad structure",
		initialStructure: initDatabaseWithBadTableStructure,
		expectError:      true,
		driverConfig:     defaultDriverConfig,
	},
}

func TestListMigrationsLog(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test for driver/mysql")
	}

	runForAllMysqlVersions(t, "ListMigrationsLog", func(t *testing.T, version string, conn *sql.DB) {
		t.Helper()

		for _, test := range listMigrationsLogTests {
			test := test
			t.Run(test.name, func(t *testing.T) {
				_, err := conn.Exec(test.initialStructure)
				if err != nil {
					t.Fatalf("error when initializing database: %s", err)
				}

				defer func() {
					_, err := conn.Exec(dropDatabase)
					if err != nil {
						t.Fatalf("falied to drop database after test: %s", err)
					}
				}()

				drv := mysql.NewDriver(conn, test.driverConfig)

				actualLog, err := drv.ListMigrationsLog()

				if test.expectError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)

					if err == nil && test.expectedLog != nil {
						assert.Equal(t, *test.expectedLog, *actualLog)
					}
				}

				runValidationStatements(t, test.validateStatements, conn)
			})
		}
	})
}

//
// --- utility stuff ---------------------
//

func runForAllMysqlVersions(t *testing.T, baseName string, test func(t *testing.T, version string, conn *sql.DB)) {
	t.Helper()

	for _, version := range versions {
		version := version
		testName := fmt.Sprintf("%s@%s", baseName, version)
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			rootPassword := randomPassword()
			t.Logf("%s - root password: %s", testName, rootPassword)

			ctx, mysqlC := makeTestContainer(t, version, rootPassword)
			defer func() {
				err := mysqlC.Terminate(ctx)
				if err != nil {
					t.Fatalf("failed to terminate test container: %s", err)
				}
			}()

			conn := connect(ctx, t, mysqlC, rootPassword)
			defer func() {
				err := conn.Close()
				if err != nil {
					t.Fatalf("failed to close connection to test database: %s", err)
				}
			}()

			test(t, version, conn)
		})
	}
}

func makeTestContainer(t *testing.T, version string, rootPassword string) (context.Context, testcontainers.Container) {
	t.Helper()

	var env map[string]string

	if strings.HasPrefix(version, "mariadb") {
		env = map[string]string{
			"MARIADB_ROOT_PASSWORD": rootPassword,
		}
	} else {
		env = map[string]string{
			"MYSQL_ROOT_PASSWORD": rootPassword,
		}
	}

	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        version,
		ExposedPorts: []string{"3306/tcp"},
		WaitingFor:   wait.ForListeningPort("3306"),
		Env:          env,
		Cmd: []string{
			"--table_definition_cache=10",
			"--performance_schema=0",
		},
	}

	mysqlC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatal(err)
	}

	return ctx, mysqlC
}

func connect(ctx context.Context, t *testing.T, mysqlC testcontainers.Container, rootPassword string) *sql.DB {
	t.Helper()

	endpoint, err := mysqlC.Endpoint(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	conn, err := sql.Open("mysql",
		fmt.Sprintf("root:%s@tcp(%s)/mysql?multiStatements=true", rootPassword, endpoint))

	if err != nil {
		t.Fatal(err)
	}

	return conn
}

func randomPassword() string {
	const length = 8
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Errorf("failed to generate a random password: %w", err))
	}
	return fmt.Sprintf("%x", b)[:length]
}

func runValidationStatements(t *testing.T, validateStatements validateStatements, conn *sql.DB) {
	t.Helper()

	for stmt, validate := range validateStatements {
		func() {
			rows, err := conn.Query(stmt)
			if err != nil {
				t.Fatalf("error when running validation statement \"%s\": %s", stmt, err)
			}
			if err = rows.Err(); err != nil {
				t.Fatalf("error when running validation statement \"%s\": %s", stmt, err)
			}
			defer rows.Close()

			validate(t, rows)
		}()
	}
}
