// SPDX-License-Identifier: Apache-2.0

package roll

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/xataio/pgroll/pkg/migrations"
)

// UnappliedMigrations returns a slice of unapplied migrations from `dir`,
// lexicographically ordered by filename without extension. Applying each of the returned
// migrations in order will bring the database up to date with `dir`.
//
// If the local order of migrations does not match the order of migrations in
// the schema history, an `ErrMismatchedMigration` error is returned.
func (m *Roll) UnappliedMigrations(ctx context.Context, dir fs.FS) ([]*migrations.RawMigration, error) {
	latestVersion, err := m.State().LatestVersion(ctx, m.Schema())
	if err != nil {
		return nil, fmt.Errorf("determining latest version: %w", err)
	}

	files, err := migrations.CollectFilesFromDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading migration files: %w", err)
	}

	history, err := m.State().SchemaHistory(ctx, m.Schema())
	if err != nil {
		return nil, fmt.Errorf("reading schema history: %w", err)
	}

	// Find the index of the first unapplied migration
	var idx int
	if latestVersion != nil {
		for _, file := range files {
			migration, err := migrations.ReadRawMigration(dir, file)
			if err != nil {
				return nil, fmt.Errorf("reading migration file %q: %w", file, err)
			}

			remoteMigration := history[idx].Migration
			if remoteMigration.Name != migration.Name {
				return nil, fmt.Errorf("%w: remote=%q, local=%q", ErrMismatchedMigration, remoteMigration.Name, migration.Name)
			}

			idx++
			if migration.Name == *latestVersion {
				break
			}
		}
	}

	// Return all unapplied migrations
	migs := make([]*migrations.RawMigration, 0, len(files))
	for _, file := range files[idx:] {
		migration, err := migrations.ReadRawMigration(dir, file)
		if err != nil {
			return nil, fmt.Errorf("reading migration file %q: %w", file, err)
		}
		migs = append(migs, migration)
	}

	return migs, nil
}
