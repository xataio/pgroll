// SPDX-License-Identifier: Apache-2.0

package roll_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
)

func TestMissingMigrations(t *testing.T) {
	t.Parallel()

	t.Run("all migrations are missing; local directory is empty", func(t *testing.T) {
		fs := fstest.MapFS{}

		testutils.WithMigratorAndConnectionToContainer(t, func(roll *roll.Roll, _ *sql.DB) {
			ctx := context.Background()

			// Apply migrations to the target database
			for _, migration := range []*migrations.Migration{
				exampleMig(t, "01_migration_1"),
				exampleMig(t, "02_migration_2"),
			} {
				err := roll.Start(ctx, migration, backfill.NewConfig())
				require.NoError(t, err)
				err = roll.Complete(ctx)
				require.NoError(t, err)
			}

			// Get missing migrations
			migs, err := roll.MissingMigrations(ctx, fs)
			require.NoError(t, err)

			// Assert that all migrations are missing in the local directory
			require.Len(t, migs, 2)
			require.Equal(t, "01_migration_1", migs[0].Name)
			require.Equal(t, "02_migration_2", migs[1].Name)
		})
	})

	t.Run("second migration is missing in the migrations directory", func(t *testing.T) {
		fs := fstest.MapFS{
			"01_migration_1.json": &fstest.MapFile{Data: exampleMigJSON(t, "01_migration_1")},
		}

		testutils.WithMigratorAndConnectionToContainer(t, func(roll *roll.Roll, _ *sql.DB) {
			ctx := context.Background()

			// Apply migrations to the target database
			for _, migration := range []*migrations.Migration{
				exampleMig(t, "01_migration_1"),
				exampleMig(t, "02_migration_2"),
			} {
				err := roll.Start(ctx, migration, backfill.NewConfig())
				require.NoError(t, err)
				err = roll.Complete(ctx)
				require.NoError(t, err)
			}

			// Get missing migrations
			migs, err := roll.MissingMigrations(ctx, fs)
			require.NoError(t, err)

			// Assert that the second migration is missing in the local directory
			require.Len(t, migs, 1)
			require.Equal(t, "02_migration_2", migs[0].Name)
		})
	})

	t.Run("all migrations are present in the migrations directory", func(t *testing.T) {
		fs := fstest.MapFS{
			"01_migration_1.json": &fstest.MapFile{Data: exampleMigJSON(t, "01_migration_1")},
			"02_migration_2.json": &fstest.MapFile{Data: exampleMigJSON(t, "02_migration_2")},
		}

		testutils.WithMigratorAndConnectionToContainer(t, func(roll *roll.Roll, _ *sql.DB) {
			ctx := context.Background()

			// Apply migrations to the target database
			for _, migration := range []*migrations.Migration{
				exampleMig(t, "01_migration_1"),
				exampleMig(t, "02_migration_2"),
			} {
				err := roll.Start(ctx, migration, backfill.NewConfig())
				require.NoError(t, err)
				err = roll.Complete(ctx)
				require.NoError(t, err)
			}

			// Get missing migrations
			migs, err := roll.MissingMigrations(ctx, fs)
			require.NoError(t, err)

			// Assert that no migrations are missing in the local directory
			require.Len(t, migs, 0)
		})
	})

	t.Run("more migrations are present in the migrations directory than on the target database", func(t *testing.T) {
		fs := fstest.MapFS{
			"01_migration_1.json": &fstest.MapFile{Data: exampleMigJSON(t, "01_migration_1")},
			"02_migration_2.json": &fstest.MapFile{Data: exampleMigJSON(t, "02_migration_2")},
			"03_migration_3.json": &fstest.MapFile{Data: exampleMigJSON(t, "03_migration_3")},
		}

		testutils.WithMigratorAndConnectionToContainer(t, func(roll *roll.Roll, _ *sql.DB) {
			ctx := context.Background()

			// Apply migrations to the target database
			for _, migration := range []*migrations.Migration{
				exampleMig(t, "01_migration_1"),
				exampleMig(t, "02_migration_2"),
			} {
				err := roll.Start(ctx, migration, backfill.NewConfig())
				require.NoError(t, err)
				err = roll.Complete(ctx)
				require.NoError(t, err)
			}

			// Get missing migrations
			migs, err := roll.MissingMigrations(ctx, fs)
			require.NoError(t, err)

			// Assert that no migrations are missing in the local directory
			require.Len(t, migs, 0)
		})
	})

	t.Run("a migration earlier in the schema history is missing locally", func(t *testing.T) {
		fs := fstest.MapFS{
			"01_migration_1.json": &fstest.MapFile{Data: exampleMigJSON(t, "01_migration_1")},
			"03_migration_3.json": &fstest.MapFile{Data: exampleMigJSON(t, "03_migration_3")},
		}

		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, _ *sql.DB) {
			ctx := context.Background()

			// Apply migrations to the target database
			for _, migration := range []*migrations.Migration{
				exampleMig(t, "01_migration_1"),
				exampleMig(t, "02_migration_2"),
				exampleMig(t, "03_migration_3"),
			} {
				err := m.Start(ctx, migration, backfill.NewConfig())
				require.NoError(t, err)
				err = m.Complete(ctx)
				require.NoError(t, err)
			}

			// Get missing migrations
			migs, err := m.MissingMigrations(ctx, fs)
			require.NoError(t, err)

			// Assert that 02_migration_2 is missing in the local directory
			require.Len(t, migs, 1)
			require.Equal(t, "02_migration_2", migs[0].Name)
		})
	})

	t.Run("no migrations have been applied to the target database", func(t *testing.T) {
		fs := fstest.MapFS{
			"01_migration_1.json": &fstest.MapFile{Data: exampleMigJSON(t, "01_migration_1")},
			"02_migration_2.json": &fstest.MapFile{Data: exampleMigJSON(t, "02_migration_2")},
		}

		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, _ *sql.DB) {
			ctx := context.Background()

			// Get missing migrations
			migs, err := m.MissingMigrations(ctx, fs)
			require.NoError(t, err)

			// Assert that no migrations are missing from the local directory
			require.Len(t, migs, 0)
		})
	})

	t.Run("migrations with no name use filename as migration name", func(t *testing.T) {
		fs := fstest.MapFS{
			"01_migration_1.json": &fstest.MapFile{Data: exampleMigJSON(t, "")},
		}

		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, _ *sql.DB) {
			ctx := context.Background()

			// Apply migrations to the target database
			for _, migration := range []*migrations.Migration{
				exampleMig(t, "01_migration_1"),
			} {
				err := m.Start(ctx, migration, backfill.NewConfig())
				require.NoError(t, err)
				err = m.Complete(ctx)
				require.NoError(t, err)
			}

			// Get missing migrations
			migs, err := m.MissingMigrations(ctx, fs)
			require.NoError(t, err)

			// Assert that no migrations are missing in the local directory; the
			// unamed migration uses the filename as the name
			require.Len(t, migs, 0)
		})
	})
}

func TestMissingMigrationsWithOldMigrationFormats(t *testing.T) {
	t.Parallel()

	t.Run("local directory contains an un-deserializable migration", func(t *testing.T) {
		fs := fstest.MapFS{
			"01_migration_1.json": &fstest.MapFile{Data: unDeserializableMigration(t, "01_migration_1")},
		}

		testutils.WithMigratorAndConnectionToContainer(t, func(roll *roll.Roll, _ *sql.DB) {
			ctx := context.Background()

			// Apply migrations to the target database
			for _, migration := range []*migrations.Migration{
				exampleMig(t, "02_migration_2"),
				exampleMig(t, "03_migration_3"),
			} {
				err := roll.Start(ctx, migration, backfill.NewConfig())
				require.NoError(t, err)
				err = roll.Complete(ctx)
				require.NoError(t, err)
			}

			// Get missing migrations
			migs, err := roll.MissingMigrations(ctx, fs)
			require.NoError(t, err)

			// Assert that migrations 2 and 3 are missing in the local directory
			require.Len(t, migs, 2)
			require.Equal(t, "02_migration_2", migs[0].Name)
			require.Equal(t, "03_migration_3", migs[1].Name)
		})
	})

	t.Run("remote migration history contains an un-deserializable migration", func(t *testing.T) {
		fs := fstest.MapFS{}

		testutils.WithMigratorAndConnectionToContainer(t, func(roll *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// Apply migrations to the target database
			for _, migration := range []*migrations.Migration{
				exampleMig(t, "01_migration_1"),
			} {
				err := roll.Start(ctx, migration, backfill.NewConfig())
				require.NoError(t, err)
				err = roll.Complete(ctx)
				require.NoError(t, err)
			}

			// Modify the first migration in the schema history to be un-deserializable; in
			// practice this could happen if the migration was applied with an older
			// version of pgroll that had a different migration format
			_, err := db.ExecContext(ctx, `UPDATE pgroll.migrations
				SET migration = REPLACE(migration::text, '"up"', '"upxxx"')::jsonb
				WHERE name = '01_migration_1'`)
			require.NoError(t, err)

			// Get missing migrations
			migs, err := roll.MissingMigrations(ctx, fs)
			require.NoError(t, err)

			// Assert that the un-deserializable migration is missing in the local directory
			require.Len(t, migs, 1)
			require.Equal(t, "01_migration_1", migs[0].Name)
		})
	})
}

func exampleMig(t *testing.T, name string) *migrations.Migration {
	t.Helper()

	migration := &migrations.Migration{
		Name: name,
		Operations: migrations.Operations{
			&migrations.OpRawSQL{
				Up: "SELECT 1",
			},
		},
	}

	return migration
}

func exampleMigJSON(t *testing.T, name string) []byte {
	t.Helper()

	migration := exampleMig(t, name)
	json, err := json.Marshal(migration)
	require.NoError(t, err)

	return json
}
