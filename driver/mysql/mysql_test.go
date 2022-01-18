//nolint:gochecknoglobals
package mysql_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/root-talis/henka/driver"
	"github.com/root-talis/henka/driver/mysql"
	"github.com/root-talis/henka/migration"
)

// mysql versions to test against
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

//
// -- bootstrap --------------------------------------
//

type testContainer struct {
	sync.Mutex
	ctx       context.Context
	container testcontainers.Container
	conn      *sql.DB
}

var containers = make(map[string]*testContainer)

func TestMain(m *testing.M) {
	failed := false
	waitGroup := sync.WaitGroup{}

	for _, version := range versions {
		version := version
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()
			if err := setupTestContainer(version); err != nil {
				failed = true
				fmt.Printf("error when creating test container for version %s: %s\n", version, err) //nolint:forbidigo
			}
		}()
	}

	waitGroup.Wait()

	var exitCode int
	if !failed {
		exitCode = m.Run()
	} else {
		exitCode = -1
	}

	for version, container := range containers {
		container := container
		version := version
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()

			if err := shutdownTestContainer(version, container); err != nil {
				fmt.Printf("error when cleaning up container %s: %s\n", version, err) //nolint:forbidigo
				exitCode = -1
			}
		}()
	}

	waitGroup.Wait()
	os.Exit(exitCode)
}

func setupTestContainer(version string) error {
	rootPassword := generateRandomPassword()
	fmt.Printf("%s - root password: %s\n", version, rootPassword) //nolint:forbidigo

	ctx, mysqlC, err := makeTestContainer(version, rootPassword)
	if err != nil {
		return fmt.Errorf("failed to create container %s: %w", version, err)
	}

	container := testContainer{
		ctx:       ctx,
		container: mysqlC,
	}
	containers[version] = &container

	conn, err := connectToDatabase(ctx, mysqlC, rootPassword)
	if err != nil {
		return fmt.Errorf("failed to connect to database in container %s: %w", version, err)
	}

	container.conn = conn
	return nil
}

func shutdownTestContainer(version string, container *testContainer) error {
	fmt.Printf("cleanup %s...\n", version) //nolint:forbidigo
	container.Lock()
	defer container.Unlock()

	if container.conn != nil {
		err := container.conn.Close()
		if err != nil {
			return fmt.Errorf("failed to close connection to test database %s: %w", version, err)
		}
	}

	err := container.container.Terminate(container.ctx)
	if err != nil {
		return fmt.Errorf("failed to terminate test container %s: %w", version, err)
	}

	fmt.Printf("cleanup %s done\n", version) //nolint:forbidigo
	return nil
}

func makeTestContainer(version string, rootPassword string) (context.Context, testcontainers.Container, error) {
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
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("3306"),
			wait.ForLog("mysqld: ready for connections"),
		),
		Env: env,
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
		return nil, nil, fmt.Errorf("error creating test container for %s: %w", version, err)
	}

	return ctx, mysqlC, nil
}

func connectToDatabase(ctx context.Context, mysqlC testcontainers.Container, rootPassword string) (*sql.DB, error) {
	endpoint, err := mysqlC.Endpoint(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("failed to get endpoint for test container: %w", err)
	}

	conn, err := sql.Open("mysql",
		fmt.Sprintf("root:%s@tcp(%s)/mysql?multiStatements=true", rootPassword, endpoint))

	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", endpoint, err)
	}

	return conn, nil
}

func generateRandomPassword() string {
	const length = 8
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Errorf("failed to generate a random password: %w", err))
	}
	return fmt.Sprintf("%x", b)[:length]
}

//
// -- templates --------------------------------------
//

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

	migrationErr1Sql = insertMigration + "(\"20220118120101\", \"createPermissionsTable\", \"x\", \"2022-01-19 10:04:00\", \"2022-01-19 10:04:01\");"

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

	initDatabaseWithMigrationsErrSet1 = initDatabaseWithEmptyTable +
		migration1Sql +
		migration2Sql +
		migrationErr1Sql +
		migration4Sql

	migrationScript1 = "CREATE TABLE users (id int not null auto_increment, primary key (id)) default charset utf8mb4"
	migrationScript2 = "DROP TABLE users"
)

func makeLogBrief(log migration.Log, startIsNull bool, endIsNull bool) logBrief {
	return logBrief{
		version:         uint64(log.Migration.Version),
		name:            log.Migration.Name,
		direction:       fmt.Sprintf("%c", log.Direction),
		startedAtIsNull: startIsNull,
		endedAtIsNull:   endIsNull,
	}
}

func makeMigrationDescr(log migration.Log, script string) migrationDescr {
	return migrationDescr{
		migration: log.Migration,
		direction: log.Direction,
		script:    script,
	}
}

type columnDescr struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default string
	Extra   string
}
type columnDescrRaw struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default *string
	Extra   *string
}

type validator = func(*testing.T, *sql.Rows)
type validateStatements = map[string]validator

var doNothing = func(t *testing.T, _ *sql.Rows) {
	t.Helper()
}

//
// --- ListMigrationsLog test ------------------------
//

// Test table for TestListMigrationsLog
var listMigrationsLogTests = []struct {
	name               string
	expectError        bool
	initialStructure   string
	driverConfig       mysql.DriverConfig
	validateStatements validateStatements
	expectedLog        *[]migration.Log
}{
	// -- success cases: ---
	/* s0 */ {
		name:             "s0 - should create new migrations_log table",
		initialStructure: initEmptyDatabase,
		driverConfig:     defaultDriverConfig,
		validateStatements: validateStatements{
			"select 1 from testDatabase.migrations_log": doNothing,
		},
		expectedLog: &[]migration.Log{}, // empty
	},
	/* s1 */ {
		name:             "s1 - should create new migrations_log table with a custom name",
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
		name:             "s2 - should not create another migrations_log table",
		initialStructure: initDatabaseWithEmptyTable,
		driverConfig:     defaultDriverConfig,
		validateStatements: map[string]func(*testing.T, *sql.Rows){
			"select 1 from testDatabase.migrations_log": doNothing,
		},
		expectedLog: &[]migration.Log{}, // empty
	},
	/* s3 */ {
		name:             "s3 - should return correct log from database",
		initialStructure: initDatabaseWithMigrationsSet1,
		driverConfig:     defaultDriverConfig,
		expectedLog:      &migrationsSet1Parsed,
	},

	// -- error cases: -----
	/* e0 */ {
		name:             "e0 - should fail if database doesn't exist",
		initialStructure: initEmptyDatabase,
		expectError:      true,
		driverConfig: mysql.DriverConfig{
			DatabaseName:        "wrongTestDatabase",
			MigrationsTableName: "migrations_log",
		},
	},
	/* e1 */ {
		name:             "e1 - should fail if migrations_log table has bad structure",
		initialStructure: initDatabaseWithBadTableStructure,
		expectError:      true,
		driverConfig:     defaultDriverConfig,
	},
	/* e2 */ {
		name:             "e2 - should fail if \"direction\" value is incorrect",
		initialStructure: initDatabaseWithMigrationsErrSet1,
		driverConfig:     defaultDriverConfig,
		expectError:      true,
	},
}

func TestListMigrationsLog(t *testing.T) { //nolint:paralleltest,tparallel
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
// --- Migrate test ----------------------------------
//

type migrationDescr struct {
	migration migration.Migration
	direction migration.Direction
	script    string
}

type logBrief struct {
	version         uint64
	name            string
	direction       string
	startedAtIsNull bool
	endedAtIsNull   bool
}

var migrateTests = []struct {
	name                 string
	expectMigrationError bool
	initialStructure     string
	driverConfig         mysql.DriverConfig
	migrations           []migrationDescr
	expectedLog          []logBrief
	validateStatements   validateStatements
	expectedTables       map[string][]columnDescr
}{
	/* s0 */ {
		name:             "s0 - should run single migration",
		initialStructure: initDatabaseWithEmptyTable,
		driverConfig:     defaultDriverConfig,
		migrations: []migrationDescr{
			makeMigrationDescr(migration1Parsed, migrationScript1),
		},
		expectedLog: []logBrief{
			makeLogBrief(migration1Parsed, false, false),
		},
		expectedTables: map[string][]columnDescr{
			"testDatabase.users": {
				{
					Field:   "id",
					Type:    "int(11)",
					Null:    "NO",
					Key:     "PRI",
					Default: "",
					Extra:   "auto_increment",
				},
			},
		},
	},
	/* s1 */ {
		name:             "s1 - should run and revert single migration",
		initialStructure: initDatabaseWithEmptyTable,
		driverConfig:     defaultDriverConfig,
		migrations: []migrationDescr{
			makeMigrationDescr(migration1Parsed, migrationScript1),
			makeMigrationDescr(migration2Parsed, migrationScript2),
		},
		expectedLog: []logBrief{
			makeLogBrief(migration1Parsed, false, false),
			makeLogBrief(migration2Parsed, false, false),
		},
		expectedTables: map[string][]columnDescr{},
	},
}

func TestMigrate(t *testing.T) { //nolint:paralleltest,tparallel
	if testing.Short() {
		t.Skip("skipping integration test for driver/mysql")
	}

	runForAllMysqlVersions(t, "Migrate", func(t *testing.T, version string, conn *sql.DB) {
		t.Helper()

		for _, test := range migrateTests {
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

				runTestMigrations(t, test.migrations, test.expectMigrationError, drv)

				actualLog := getMigrationsLog(t, conn)
				assert.Equal(t, test.expectedLog, actualLog)

				actualTables := getAllTables(t, conn)
				assert.Equal(t, test.expectedTables, actualTables)

				runValidationStatements(t, test.validateStatements, conn)
			})
		}
	})
}

func runTestMigrations(t *testing.T, migrations []migrationDescr, expectMigrationError bool, drv driver.Driver) {
	t.Helper()

	for _, mig := range migrations {
		err := drv.Migrate(mig.migration, mig.direction, mig.script)

		if expectMigrationError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

//
// --- utility stuff ---------------------------------
//

func runForAllMysqlVersions(t *testing.T, baseName string, test func(t *testing.T, version string, conn *sql.DB)) {
	t.Helper()

	for version, container := range containers {
		container := container
		version := version
		testName := fmt.Sprintf("%s@%s", baseName, version)
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			container.Lock()
			defer container.Unlock()

			test(t, version, container.conn)
		})
	}
}

func getMigrationsLog(t *testing.T, conn *sql.DB) []logBrief {
	t.Helper()
	result := make([]logBrief, 0, 8)

	rows, err := conn.Query("SELECT version, migration_name, direction, start_time is null, end_time is null FROM testDatabase.migrations_log ORDER BY id")
	if err != nil {
		t.Fatalf("failed to query migartions log: %s", err)
	}
	if err = rows.Err(); err != nil {
		t.Fatalf("failed to query migartions log: %s", err)
	}

	defer rows.Close()

	for rows.Next() {
		row := logBrief{}
		err := rows.Scan(
			&row.version,
			&row.name,
			&row.direction,
			&row.startedAtIsNull,
			&row.endedAtIsNull,
		)
		if err != nil {
			t.Fatalf("failed to scan migrations log: %s", err)
		}

		result = append(result, row)
	}

	return result
}

func getAllTables(t *testing.T, conn *sql.DB) map[string][]columnDescr {
	result := make(map[string][]columnDescr)

	rows, err := conn.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'testDatabase'")
	if err != nil {
		t.Fatalf("failed to query a list of tables: %s", err)
	}
	if err = rows.Err(); err != nil {
		t.Fatalf("failed to query a list of tables: %s", err)
	}

	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			t.Fatalf("failed to scan a list of tables: %s", err)
		}

		if name == "migrations_log" {
			continue
		}

		name = fmt.Sprintf("testDatabase.%s", name)
		result[name] = getTableStructure(t, name, conn)
	}

	return result
}

func getTableStructure(t *testing.T, tableName string, conn *sql.DB) []columnDescr {
	t.Helper()

	rows, err := conn.Query(fmt.Sprintf("desc %s", tableName))
	if err != nil {
		t.Fatalf("error when querying table structure for %s: %s", tableName, err)
	}
	if err = rows.Err(); err != nil {
		t.Fatalf("error when querying table structure for %s: %s", tableName, err)
	}
	defer rows.Close()

	var structure []columnDescr

	for rows.Next() {
		var descrRaw columnDescrRaw
		err := rows.Scan(
			&descrRaw.Field,
			&descrRaw.Type,
			&descrRaw.Null,
			&descrRaw.Key,
			&descrRaw.Default,
			&descrRaw.Extra,
		)
		if err != nil {
			t.Fatalf("failed to scan table description: %s", err)
		}

		descr := parseRawColumnDescription(descrRaw)
		structure = append(structure, unifyColumnDescription(descr))
	}

	return structure
}

func parseRawColumnDescription(descrRaw columnDescrRaw) columnDescr {
	def := ""
	extra := ""
	if descrRaw.Default != nil {
		def = *descrRaw.Default
	}
	if descrRaw.Extra != nil {
		extra = *descrRaw.Extra
	}

	descr := columnDescr{
		Field:   descrRaw.Field,
		Type:    descrRaw.Type,
		Null:    descrRaw.Null,
		Key:     descrRaw.Key,
		Default: def,
		Extra:   extra,
	}

	return descr
}

func unifyColumnDescription(descr columnDescr) columnDescr {
	if descr.Type == "int" { // mysql:8.0 returns int instead of int(11)
		descr.Type = "int(11)"
	}

	return descr
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
