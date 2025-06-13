// SPDX-License-Identifier: Apache-2.0

package roll

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/xataio/pgroll/pkg/migrations"
)

var (
	ErrNoMigrationFiles   = fmt.Errorf("no migration files found")
	ErrNoMigrationApplied = fmt.Errorf("no migrations applied")
)

// LatestVersionLocal returns the version schema name of the last migration in
// `dir`, where the migration files are lexicographically ordered by filename.
func LatestVersionLocal(ctx context.Context, dir fs.FS) (string, error) {
	migration, err := latestMigrationLocal(dir)
	if err != nil {
		return "", fmt.Errorf("getting latest local migration: %w", err)
	}

	return migration.VersionSchemaName(), nil
}

// LatestMigrationNameLocal returns the name of the last migration in `dir`,
// where the migration files are lexicographically ordered by filename.
func LatestMigrationNameLocal(ctx context.Context, dir fs.FS) (string, error) {
	migration, err := latestMigrationLocal(dir)
	if err != nil {
		return "", fmt.Errorf("getting latest local migration: %w", err)
	}

	return migration.Name, nil
}

// LatestVersionRemote returns the version schema name of the last migration to
// have been applied to the target schema.
func (m *Roll) LatestVersionRemote(ctx context.Context) (string, error) {
	latestVersion, err := m.State().LatestVersion(ctx, m.Schema())
	if err != nil {
		return "", fmt.Errorf("failed to get latest version: %w", err)
	}

	if latestVersion == nil {
		return "", ErrNoMigrationApplied
	}

	return *latestVersion, nil
}

// LatestMigrationNameRemote returns the name of the last migration to have been
// applied to the target schema.
func (m *Roll) LatestMigrationNameRemote(ctx context.Context) (string, error) {
	latestName, err := m.State().LatestMigration(ctx, m.Schema())
	if err != nil {
		return "", fmt.Errorf("failed to get latest migration name: %w", err)
	}

	if latestName == nil {
		return "", ErrNoMigrationApplied
	}

	return *latestName, nil
}

// latestMigrationLocal returns the latest migration from the local migration
// directory, where the migration files are lexicographically ordered by
// filename.
func latestMigrationLocal(dir fs.FS) (*migrations.Migration, error) {
	files, err := migrations.CollectFilesFromDir(dir)
	if err != nil {
		return nil, fmt.Errorf("getting migration files from dir: %w", err)
	}

	if len(files) == 0 {
		return nil, ErrNoMigrationFiles
	}

	latest := files[len(files)-1]

	migration, err := migrations.ReadMigration(dir, latest)
	if err != nil {
		return nil, fmt.Errorf("reading migration file %q: %w", latest, err)
	}

	return migration, nil
}
