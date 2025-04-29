// SPDX-License-Identifier: Apache-2.0

package roll

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/xataio/pgroll/pkg/migrations"
)

// MissingMigrations returns the slice of migrations that have been applied to
// the target database but are missing from the local migrations directory
// `dir`. If the local order of migrations does not match the order of
// migrations in the schema history, an `ErrMismatchedMigration` error is
// returned.
func (m *Roll) MissingMigrations(ctx context.Context, dir fs.FS) ([]*migrations.Migration, error) {
	// Determine the latest version of the database
	latestVersion, err := m.State().LatestVersion(ctx, m.Schema())
	if err != nil {
		return nil, fmt.Errorf("determining latest version: %w", err)
	}

	// If no migrations are applied, return a nil slice
	if latestVersion == nil {
		return nil, nil
	}

	// Collect all migration files from the directory
	files, err := migrations.CollectFilesFromDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading migration files: %w", err)
	}

	// Get the full schema history from the database
	history, err := m.State().SchemaHistory(ctx, m.Schema())
	if err != nil {
		return nil, fmt.Errorf("reading schema history: %w", err)
	}

	// Ensure that the schema history and the local migration files are in the
	// same order up to the latest version applied to the database
	for idx, h := range history {
		if idx >= len(files) {
			break
		}

		localMigration, err := migrations.ReadMigration(dir, files[idx])
		if err != nil {
			return nil, fmt.Errorf("failed to read migration file %q: %w", h.Migration.Name, err)
		}
		remoteMigration := h.Migration

		if remoteMigration.Name != localMigration.Name {
			return nil, fmt.Errorf("%w: remote=%q, local=%q", ErrMismatchedMigration, remoteMigration.Name, localMigration.Name)
		}
	}

	// Return all the missing migrations
	migs := make([]*migrations.Migration, 0, len(history))
	for i := len(files); i < len(history); i++ {
		migs = append(migs, &history[i].Migration)
	}

	return migs, nil
}
