package mig

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

type fileSource struct {
	migrationsDir string
}

const versionLength = 14

var (
	ErrMigrationsDirectoryIsNotADirectory = errors.New("migrationsDirectory is not a directory")
)

func NewFileSource(migrationsDirectory string) (Source, error) {
	stat, err := os.Stat(migrationsDirectory)

	if os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to stat migrations directory: %w", err)
	}

	if !stat.IsDir() {
		return nil, ErrMigrationsDirectoryIsNotADirectory
	}

	return &fileSource{
		migrationsDir: migrationsDirectory,
	}, nil
}

func (rdr *fileSource) GetAvailableMigrations() (*[]MigrationDescription, error) {
	dirEntries, err := os.ReadDir(rdr.migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read contents of migrations directory: %w", err)
	}

	// find all suitable migrations and build a collection of descriptions
	migrations := make(versionMap)
	for _, entry := range dirEntries {
		if entry.IsDir() || !entry.Type().IsRegular() {
			continue
		}

		fileName := entry.Name()
		migration, err := getValidMigrationFromFileName(fileName)
		if err != nil {
			continue
		}

		if strings.HasSuffix(fileName, ".up.sql") {
			err = migrations.updateDescription(migration, Up)
		} else if strings.HasSuffix(fileName, ".down.sql") {
			err = migrations.updateDescription(migration, Down)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to parse directory entries: %w", err)
		}
	}

	keys := getSortedVersions(migrations)
	result := buildMigrationsSlice(keys, migrations)

	return &result, nil
}

func getSortedVersions(migrations versionMap) []int {
	keys := make([]int, 0, len(migrations))

	for k := range migrations {
		keys = append(keys, int(k))
	}

	sort.Ints(keys)

	return keys
}

func buildMigrationsSlice(keys []int, migrations versionMap) []MigrationDescription {
	result := make([]MigrationDescription, len(keys))
	for i, k := range keys {
		result[i] = migrations[Version(k)]
	}
	return result
}

type versionMap map[Version]MigrationDescription

func (m *versionMap) updateDescription(migration Migration, direction Direction) error {
	version, exists := (*m)[migration.Version]

	switch {
	case !exists:
		(*m)[migration.Version] = MigrationDescription{
			Migration: migration,
			CanUndo:   direction == Down,
		}

	case version.Name != migration.Name:
		return fmt.Errorf(
			"migration %d already exists with name \"%s\" (new name \"%s\" is encountered)",
			migration.Version,
			version.Name,
			migration.Name,
		)

	case direction == Down:
		version.CanUndo = true
		(*m)[migration.Version] = version
	}

	return nil
}

func getValidMigrationFromFileName(fileName string) (Migration, error) {
	if !strings.HasPrefix(fileName, "V") {
		return Migration{}, fmt.Errorf("migration file name is invalid: %s", fileName)
	}

	migrationFullName := strings.TrimPrefix(fileName, "V")
	migrationFullName = strings.TrimSuffix(migrationFullName, ".up.sql")
	migrationFullName = strings.TrimSuffix(migrationFullName, ".down.sql")

	asRunes := []rune(migrationFullName)

	if len(asRunes) < versionLength+1 {
		return Migration{}, fmt.Errorf("migration file name is too short to be valid: %s", fileName)
	}

	version := asRunes[:versionLength]

	for _, c := range version {
		if !unicode.IsDigit(c) {
			return Migration{}, fmt.Errorf(
				"migration file name does not contain a valid version (symbol \"%c\" is not allowed): %s",
				c,
				fileName,
			)
		}
	}

	v := string(version)
	versionAsInt, err := strconv.ParseUint(v, 0, VersionBits)
	if err != nil {
		return Migration{}, fmt.Errorf("migration file name does not contain a valid version: %s", fileName)
	}

	nameAsRunes := asRunes[versionLength:]
	if nameAsRunes[0] != '_' {
		return Migration{}, fmt.Errorf("migration file is missing an underscore after version (%c given): %s", nameAsRunes[0], fileName)
	}

	name := strings.TrimPrefix(string(nameAsRunes), "_")

	return Migration{
		Version: Version(versionAsInt),
		Name:    name,
	}, nil
}

func (rdr *fileSource) ReadMigration(version Version, direction Direction) (io.Reader, error) {
	return nil, nil
}
