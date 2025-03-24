// SPDX-License-Identifier: Apache-2.0

package roll_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"maps"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
)

func TestUnappliedMigrations(t *testing.T) {
	t.Parallel()

	t.Run("all migrations are unapplied", func(t *testing.T) {
		fs := fstest.MapFS{
			"01_migration_1.json": &fstest.MapFile{Data: exampleMigration(t, "01_migration_1")},
			"02_migration_2.json": &fstest.MapFile{Data: exampleMigration(t, "02_migration_2")},
		}

		testutils.WithMigratorAndConnectionToContainer(t, func(roll *roll.Roll, _ *sql.DB) {
			ctx := context.Background()

			// Get migrations to apply
			migs, err := roll.UnappliedMigrations(ctx, fs)
			require.NoError(t, err)

			// Assert that all migrations are unapplied
			require.Len(t, migs, 2)
			require.Equal(t, "01_migration_1", migs[0].Name)
			require.Equal(t, "02_migration_2", migs[1].Name)
		})
	})

	t.Run("all but the first migration are unapplied", func(t *testing.T) {
		fs := fstest.MapFS{
			"01_migration_1.json": &fstest.MapFile{Data: exampleMigration(t, "01_migration_1")},
			"02_migration_2.json": &fstest.MapFile{Data: exampleMigration(t, "02_migration_2")},
			"03_migration_3.json": &fstest.MapFile{Data: exampleMigration(t, "03_migration_3")},
		}

		testutils.WithMigratorAndConnectionToContainer(t, func(roll *roll.Roll, _ *sql.DB) {
			ctx := context.Background()

			// Unmarshal the first migration from the migrations directory
			var migration migrations.Migration
			err := json.Unmarshal(fs["01_migration_1.json"].Data, &migration)
			require.NoError(t, err)

			// Apply the first migration
			err = roll.Start(ctx, &migration, backfill.NewConfig())
			require.NoError(t, err)
			err = roll.Complete(ctx)
			require.NoError(t, err)

			// Get migrations to apply
			migs, err := roll.UnappliedMigrations(ctx, fs)
			require.NoError(t, err)

			// Assert that the second and third migrations are unapplied
			require.Len(t, migs, 2)
			require.Equal(t, "02_migration_2", migs[0].Name)
			require.Equal(t, "03_migration_3", migs[1].Name)
		})
	})

	t.Run("all migrations are applied", func(t *testing.T) {
		fs := fstest.MapFS{
			"01_migration_1.json": &fstest.MapFile{Data: exampleMigration(t, "01_migration_1")},
			"02_migration_2.json": &fstest.MapFile{Data: exampleMigration(t, "02_migration_2")},
			"03_migration_3.json": &fstest.MapFile{Data: exampleMigration(t, "03_migration_3")},
		}

		testutils.WithMigratorAndConnectionToContainer(t, func(roll *roll.Roll, _ *sql.DB) {
			ctx := context.Background()

			// Unmarshal and apply all migrations from the migrations directory
			for _, filename := range slices.Sorted(maps.Keys(fs)) {
				var migration migrations.Migration
				err := json.Unmarshal(fs[filename].Data, &migration)
				require.NoError(t, err)

				err = roll.Start(ctx, &migration, backfill.NewConfig())
				require.NoError(t, err)
				err = roll.Complete(ctx)
				require.NoError(t, err)
			}

			// Get migrations to apply
			migs, err := roll.UnappliedMigrations(ctx, fs)
			require.NoError(t, err)

			// Assert that no migrations are unapplied
			require.Len(t, migs, 0)
		})
	})

	t.Run("remote migration history does not match local history", func(t *testing.T) {
		fs := fstest.MapFS{
			"01_migration_1.json": &fstest.MapFile{Data: exampleMigration(t, "01_migration_1")},
			"02_migration_2.json": &fstest.MapFile{Data: exampleMigration(t, "02_migration_2")},
		}

		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, _ *sql.DB) {
			ctx := context.Background()

			// Apply a migration that does not exist in the migrations directory
			err := m.Start(ctx, &migrations.Migration{
				Name: "01a_migration_1a",
				Operations: migrations.Operations{
					&migrations.OpRawSQL{Up: "SELECT 1"},
				},
			}, backfill.NewConfig())
			require.NoError(t, err)
			err = m.Complete(ctx)
			require.NoError(t, err)

			// Get migrations to apply
			_, err = m.UnappliedMigrations(ctx, fs)

			// Assert that a mismatched migration error is returned
			assert.ErrorIs(t, err, roll.ErrMismatchedMigration)
		})
	})

	t.Run("empty directory has no unapplied migrations", func(t *testing.T) {
		fs := fstest.MapFS{}

		testutils.WithMigratorAndConnectionToContainer(t, func(roll *roll.Roll, _ *sql.DB) {
			ctx := context.Background()

			// Get migrations to apply
			migs, err := roll.UnappliedMigrations(ctx, fs)
			require.NoError(t, err)

			// Assert that there are no unapplied migrations
			require.Len(t, migs, 0)
		})
	})

	t.Run("migrations with no name use filename as migration name", func(t *testing.T) {
		// Create migration JSON without a name field
		migrationWithoutName := migrations.Migration{
			Operations: migrations.Operations{
				&migrations.OpRawSQL{Up: "SELECT 1"},
			},
		}
		migBytes, err := json.Marshal(migrationWithoutName)
		require.NoError(t, err)

		fs := fstest.MapFS{
			"03_unnamed_migration.json": &fstest.MapFile{Data: migBytes},
		}

		testutils.WithMigratorAndConnectionToContainer(t, func(roll *roll.Roll, _ *sql.DB) {
			ctx := context.Background()

			// Get migrations to apply
			migs, err := roll.UnappliedMigrations(ctx, fs)
			require.NoError(t, err)

			// Assert that the migration name is taken from the filename
			require.Len(t, migs, 1)
			require.Equal(t, "03_unnamed_migration", migs[0].Name)
		})
	})
}

func exampleMigration(t *testing.T, name string) []byte {
	t.Helper()

	mig := &migrations.Migration{
		Name: name,
		Operations: migrations.Operations{
			&migrations.OpRawSQL{Up: "SELECT 1"},
		},
	}

	bytes, err := json.Marshal(mig)
	require.NoError(t, err)

	return bytes
}
