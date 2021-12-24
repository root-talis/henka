package henka

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

type filesSource struct {
	migrationsDir string
	fs            fs.FS
}

const versionLength = 14

var (
	ErrMigrationsDirectoryIsNotADirectory = errors.New("migrationsDirectory is not a directory")
	ErrMigrationFileNameIsInvalid         = errors.New("migration file name is invalid")
)

func NewFilesSource(fileSystem fs.FS, migrationsDirectory string) (Source, error) {
	stat, err := fs.Stat(fileSystem, migrationsDirectory)

	if err != nil {
		return nil, fmt.Errorf("failed to stat migrations directory: %w", err)
	}

	if !stat.IsDir() {
		return nil, ErrMigrationsDirectoryIsNotADirectory
	}

	return &filesSource{
		migrationsDir: migrationsDirectory,
		fs:            fileSystem,
	}, nil
}

func (rdr *filesSource) GetAvailableMigrations() (*[]MigrationDescription, error) {
	dirEntries, err := fs.ReadDir(rdr.fs, rdr.migrationsDir)
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
			"%w: version %d has conflicting names: \"%s\" and \"%s\"",
			ErrMigrationDuplicated,
			migration.Version,
			version.Name,
			migration.Name,
		)

	// IDK how to get this case to show up in coverage.
	// It would only be reached if file names are fetched from FS in reverse order.
	case direction == Down:
		version.CanUndo = true
		(*m)[migration.Version] = version
	}

	return nil
}

func getValidMigrationFromFileName(fileName string) (Migration, error) {
	if !strings.HasPrefix(fileName, "V") {
		return Migration{}, fmt.Errorf("%w: %s", ErrMigrationFileNameIsInvalid, fileName)
	}

	migrationFullName := strings.TrimPrefix(fileName, "V")
	migrationFullName = strings.TrimSuffix(migrationFullName, ".up.sql")
	migrationFullName = strings.TrimSuffix(migrationFullName, ".down.sql")

	asRunes := []rune(migrationFullName)

	if len(asRunes) < versionLength+1 {
		return Migration{}, fmt.Errorf("%w: %s is too short", ErrMigrationFileNameIsInvalid, fileName)
	}

	version := asRunes[:versionLength]
	v := string(version)
	versionAsInt, err := strconv.ParseUint(v, 0, VersionBits)
	if err != nil {
		return Migration{}, fmt.Errorf("%w: %s does not contain a valid version", ErrMigrationFileNameIsInvalid, fileName)
	}

	nameAsRunes := asRunes[versionLength:]
	if nameAsRunes[0] != '_' {
		return Migration{}, fmt.Errorf("%w: %s is missing an underscore after version (%c given)",
			ErrMigrationFileNameIsInvalid, fileName, nameAsRunes[0])
	}

	name := strings.TrimPrefix(string(nameAsRunes), "_")
	if len(name) == 0 {
		return Migration{}, fmt.Errorf("%w: %s is missing name section", ErrMigrationFileNameIsInvalid, fileName)
	}

	return Migration{
		Version: Version(versionAsInt),
		Name:    name,
	}, nil
}

func (rdr *filesSource) ReadMigration(migration Migration, direction Direction) (io.Reader, error) {
	return nil, nil
}
