// SPDX-License-Identifier: Apache-2.0

package roll_test

import (
	"context"
	"database/sql"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
)

func TestLatestVersionLocal(t *testing.T) {
	t.Parallel()

	t.Run("returns the name of the last migration in the directory", func(t *testing.T) {
		fs := fstest.MapFS{
			"01_migration_1.json": &fstest.MapFile{Data: exampleMigration(t, "01_migration_1")},
			"02_migration_2.json": &fstest.MapFile{Data: exampleMigration(t, "02_migration_2")},
			"03_migration_3.json": &fstest.MapFile{Data: exampleMigration(t, "03_migration_3")},
		}

		ctx := context.Background()

		// Get the latest migration in the directory
		latest, err := roll.LatestVersionLocal(ctx, fs)
		require.NoError(t, err)

		// Assert last migration name
		assert.Equal(t, "03_migration_3", latest)
	})

	t.Run("returns an error if the directory is empty", func(t *testing.T) {
		fs := fstest.MapFS{}

		ctx := context.Background()

		// Get the latest migration in the directory
		_, err := roll.LatestVersionLocal(ctx, fs)

		// Assert expected error
		assert.ErrorIs(t, err, roll.ErrNoMigrationFiles)
	})
}

func TestLatestVersionRemote(t *testing.T) {
	t.Parallel()

	t.Run("returns the name of the latest version in the target schema", func(t *testing.T) {
		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, _ *sql.DB) {
			ctx := context.Background()

			// Start and complete a migration
			err := m.Start(ctx, &migrations.Migration{
				Name: "01_first_migration",
				Operations: migrations.Operations{
					&migrations.OpRawSQL{Up: "SELECT 1"},
				},
			}, backfill.NewConfig())
			require.NoError(t, err)
			err = m.Complete(ctx)
			require.NoError(t, err)

			// Get the latest version in the target schema
			latestVersion, err := m.LatestVersionRemote(ctx)
			require.NoError(t, err)

			// Assert latest migration name
			assert.Equal(t, "01_first_migration", latestVersion)
		})
	})

	t.Run("returns an error if no migrations have been applied", func(t *testing.T) {
		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, _ *sql.DB) {
			ctx := context.Background()

			// Get the latest migration in the directory
			_, err := m.LatestVersionRemote(ctx)

			// Assert expected error
			assert.ErrorIs(t, err, roll.ErrNoMigrationApplied)
		})
	})
}
