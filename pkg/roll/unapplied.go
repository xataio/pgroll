// SPDX-License-Identifier: Apache-2.0

package roll

import (
	"context"
	"fmt"
	"io/fs"
	"sort"

	"github.com/xataio/pgroll/pkg/migrations"
)

// UnappliedMigrations returns the slice of unapplied migrations from `dir`
// that have not yet been applied to the database. Applying each of the
// returned migrations in order will bring the database up to date with `dir`.
//
// If the local order of migrations does not match the order of migrations in
// the schema history, an `ErrMismatchedMigration` error is returned.
func (m *Roll) UnappliedMigrations(ctx context.Context, dir fs.FS) ([]*migrations.RawMigration, error) {
	history, err := m.State().SchemaHistory(ctx, m.Schema())
	if err != nil {
		return nil, fmt.Errorf("reading schema history: %w", err)
	}

	baseline, err := m.State().LatestBaseline(ctx, m.Schema())
	if err != nil {
		return nil, fmt.Errorf("reading baseline: %w", err)
	}

	// Get all local migration files
	files, err := migrations.CollectFilesFromDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading migration files: %w", err)
	}

	baselineName := ""
	if baseline != nil {
		baselineName = baseline.Name
	}

	// Find the index of the first local migration after the baseline
	filesStartIdx := sort.Search(len(files), func(i int) bool {
		var migration *migrations.RawMigration
		migration, err = migrations.ReadRawMigration(dir, files[i])
		if err != nil {
			return false
		}
		return migration.Name > baselineName
	})
	if err != nil {
		return nil, fmt.Errorf("finding migration after baseline: %w", err)
	}

	// Read all migrations that come after the baseline
	migsAfterBaseline := make([]*migrations.RawMigration, 0, len(files))
	for _, file := range files[filesStartIdx:] {
		migration, err := migrations.ReadRawMigration(dir, file)
		if err != nil {
			return nil, fmt.Errorf("reading migration file %q: %w", file, err)
		}
		migsAfterBaseline = append(migsAfterBaseline, migration)
	}

	// Find the index of the first local migration that has not been applied to
	// the database and ensure that the order of migrations in the database
	// matches the order of migrations in the local directory.
	var appliedCount int
	for _, m := range migsAfterBaseline {
		// Stop when we've checked all the migrations in history
		if appliedCount >= len(history) {
			break
		}

		remoteMigration := history[appliedCount].Migration
		if remoteMigration.Name != m.Name {
			return nil, fmt.Errorf("%w: remote=%q, local=%q",
				ErrMismatchedMigration, remoteMigration.Name, m.Name)
		}

		appliedCount++
	}

	// Return only the migrations that haven't been applied yet
	return migsAfterBaseline[appliedCount:], nil
}
