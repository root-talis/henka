package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/root-talis/henka/driver"
	"github.com/root-talis/henka/migration"
)

type DriverConfig struct {
	DatabaseName        string
	MigrationsTableName string
}

type mysqlDriver struct {
	conn   *sql.DB
	config DriverConfig
}

func NewMysqlDriver(conn *sql.DB, config DriverConfig) driver.Driver {
	return &mysqlDriver{
		conn:   conn,
		config: config,
	}
}

func (drv *mysqlDriver) ListMigrationsLog() (*[]migration.Log, error) {
	tableName := drv.makeEscapedMigrationsTableName()

	if err := drv.ensureMigrationsTableExists(&tableName); err != nil {
		return nil, fmt.Errorf("failed to list applied versions: %w", err)
	}

	rows, err := drv.conn.Query(fmt.Sprintf(
		"SELECT version, migration_name, direction, start_time FROM %s ORDER BY id",
		tableName,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to list applied versions: %w", err)
	}
	defer rows.Close()

	result := make([]migration.Log, 0)
	for rows.Next() {
		var log migration.Log
		var appliedAt string
		var direction string

		rows.Scan(
			&log.Version,
			&log.Name,
			&direction,
			&appliedAt,
		)

		switch strings.ToLower(direction) {
		case "u":
			log.Direction = migration.Up
		case "d":
			log.Direction = migration.Down
		default:
			return nil, fmt.Errorf("%w: direction \"%s\" is unknown", driver.ErrInvalidLogTable, direction)
		}

		log.AppliedAt, err = time.Parse("2006-01-02 15:04:05", appliedAt)
		if err != nil {
			log.AppliedAt = time.Time{}
		}

		result = append(result, log)
	}

	return &result, nil
}

func (drv *mysqlDriver) makeEscapedMigrationsTableName() string {
	return fmt.Sprintf(
		"`%s`.`%s`",
		escapeMysqlString(drv.config.DatabaseName),
		escapeMysqlString(drv.config.MigrationsTableName),
	)
}

func (drv *mysqlDriver) ensureMigrationsTableExists(escapedTableName *string) error {
	_, err := drv.conn.Exec(fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s ("+
			"id             int not null auto_increment, "+
			"version        bigint, "+
			"migration_name varchar(100) null, "+
			"direction      char(1) null, "+ // "u" or "d"
			"start_time     timestamp default CURRENT_TIMESTAMP not null, "+
			"end_time       timestamp default '0000-00-00 00:00:00' not null, "+
			"primary key (id)"+
			") default charset utf8",
		*escapedTableName,
	))

	if err != nil {
		return fmt.Errorf("failed to create migrations table %s: %w", *escapedTableName, err)
	}

	return nil
}

// originally from https://gist.github.com/siddontang/8875771
func escapeMysqlString(sql string) string { //nolint:cyclop
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
