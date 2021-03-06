package files

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/root-talis/henka/migration"
	"github.com/root-talis/henka/source"
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

func NewFilesSource(fileSystem fs.FS, migrationsDirectory string) (source.Source, error) {
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

func (rdr *filesSource) GetAvailableMigrations() (*[]migration.Description, error) {
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
		mig, err := getValidMigrationFromFileName(fileName)
		if err != nil {
			continue
		}

		if strings.HasSuffix(fileName, ".up.hmf") {
			err = migrations.updateDescription(mig, migration.Up)
		} else if strings.HasSuffix(fileName, ".down.hmf") {
			err = migrations.updateDescription(mig, migration.Down)
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

func buildMigrationsSlice(keys []int, migrations versionMap) []migration.Description {
	result := make([]migration.Description, len(keys))
	for i, k := range keys {
		result[i] = migrations[migration.Version(k)]
	}
	return result
}

type versionMap map[migration.Version]migration.Description

func (m *versionMap) updateDescription(mig migration.Migration, direction migration.Direction) error {
	version, exists := (*m)[mig.Version]

	switch {
	case !exists:
		(*m)[mig.Version] = migration.Description{
			Migration: mig,
			CanUndo:   direction == migration.Down,
		}

	case version.Name != mig.Name:
		return fmt.Errorf(
			"%w: version %d has conflicting names: \"%s\" and \"%s\"",
			source.ErrMigrationDuplicated,
			mig.Version,
			version.Name,
			mig.Name,
		)

	// IDK how to get this case to show up in coverage.
	// It would only be reached if file names are fetched from FS in reverse order.
	case direction == migration.Down:
		version.CanUndo = true
		(*m)[mig.Version] = version
	}

	return nil
}

func getValidMigrationFromFileName(fileName string) (migration.Migration, error) {
	if !strings.HasPrefix(fileName, "V") {
		return migration.Migration{}, fmt.Errorf("%w: %s", ErrMigrationFileNameIsInvalid, fileName)
	}

	migrationFullName := strings.TrimPrefix(fileName, "V")
	migrationFullName = strings.TrimSuffix(migrationFullName, ".up.hmf")
	migrationFullName = strings.TrimSuffix(migrationFullName, ".down.hmf")

	asRunes := []rune(migrationFullName)

	if len(asRunes) < versionLength+1 {
		return migration.Migration{}, fmt.Errorf("%w: %s is too short", ErrMigrationFileNameIsInvalid, fileName)
	}

	version := asRunes[:versionLength]
	v := string(version)
	versionAsInt, err := strconv.ParseUint(v, 0, migration.VersionBits)
	if err != nil {
		return migration.Migration{}, fmt.Errorf("%w: %s does not contain a valid version", ErrMigrationFileNameIsInvalid, fileName)
	}

	nameAsRunes := asRunes[versionLength:]
	if nameAsRunes[0] != '_' {
		return migration.Migration{}, fmt.Errorf("%w: %s is missing an underscore after version (%c given)",
			ErrMigrationFileNameIsInvalid, fileName, nameAsRunes[0])
	}

	name := strings.TrimPrefix(string(nameAsRunes), "_")
	if len(name) == 0 {
		return migration.Migration{}, fmt.Errorf("%w: %s is missing name section", ErrMigrationFileNameIsInvalid, fileName)
	}

	return migration.Migration{
		Version: migration.Version(versionAsInt),
		Name:    name,
	}, nil
}

func (rdr *filesSource) ReadMigration(migration migration.Migration, direction migration.Direction) (io.Reader, error) {
	return nil, nil
}
