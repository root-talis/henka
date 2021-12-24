package mig

import (
	"database/sql"
	"fmt"
	"time"
)

type MysqlDriverConfig struct {
	DatabaseName        string
	MigrationsTableName string
}

type mysqlDriver struct {
	conn   *sql.DB
	config MysqlDriverConfig
}

func NewMysqlMigDriver(conn *sql.DB, config MysqlDriverConfig) Driver {
	return &mysqlDriver{
		conn:   conn,
		config: config,
	}
}

func (driver *mysqlDriver) ListAppliedMigrations() (*[]MigrationState, error) {
	tableName := driver.makeEscapedMigrationsTableName()

	if err := driver.ensureMigrationsTableExists(&tableName); err != nil {
		return nil, fmt.Errorf("failed to list applied versions: %w", err)
	}

	rows, err := driver.conn.Query(fmt.Sprintf(
		"SELECT version, migration_name, start_time FROM %s ORDER BY version",
		tableName,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to list applied versions: %w", err)
	}
	defer rows.Close()

	result := make([]MigrationState, 0)
	for rows.Next() {
		state := MigrationState{
			Status: MigrationApplied,
		}

		var appliedAt string

		rows.Scan(
			&state.Version,
			&state.Name,
			&appliedAt,
		)

		state.AppliedAt, err = time.Parse("2006-01-02 15:04:05", appliedAt)
		if err != nil {
			state.AppliedAt = time.Time{}
		}

		result = append(result, state)
	}

	return &result, nil
}

func (driver *mysqlDriver) makeEscapedMigrationsTableName() string {
	return fmt.Sprintf(
		"`%s`.`%s`",
		escapeMysqlString(driver.config.DatabaseName),
		escapeMysqlString(driver.config.MigrationsTableName),
	)
}

func (driver *mysqlDriver) ensureMigrationsTableExists(escapedTableName *string) error {
	_, err := driver.conn.Exec(fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s ("+
			"version bigint, "+
			"migration_name varchar(100) null, "+
			"start_time timestamp default CURRENT_TIMESTAMP not null, "+
			"end_time   timestamp default '0000-00-00 00:00:00' not null"+
			") default charset utf8",
		*escapedTableName,
	))

	if err != nil {
		return fmt.Errorf("failed to create migrations table %s: %w", *escapedTableName, err)
	}

	return nil
}

// originally from https://gist.github.com/siddontang/8875771
func escapeMysqlString(sql string) string {
	dest := make([]rune, 0, 2*len(sql))

	for _, character := range sql {
		var escape rune

		switch character {
		case 0:
			escape = '0'
		case '\n':
			escape = 'n'
		case '\r':
			escape = 'r'
		case '\\':
			escape = '\\'
		case '\'':
			escape = '\''
		case '"':
			escape = '"'
		case '`':
			escape = '`'
		case '\032':
			escape = 'Z'
		}

		if escape != 0 {
			dest = append(dest, '\\', escape)
		} else {
			dest = append(dest, character)
		}
	}

	return string(dest)
}
