// SPDX-License-Identifier: Apache-2.0

package state_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
)

func TestLatestAndPreviousMigrationMethods(t *testing.T) {
	t.Parallel()

	t.Run("no migrations applied", func(t *testing.T) {
		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// Get the latest migration name
			latest, err := m.State().LatestMigration(ctx, "public")
			require.NoError(t, err)

			// Get the previous migration name
			previous, err := m.State().PreviousMigration(ctx, "public")
			require.NoError(t, err)

			// Assert that both latest and previous are nil as no migrations have
			// been applied
			require.Nil(t, latest)
			require.Nil(t, previous)
		})
	})

	t.Run("one pgroll migration applied", func(t *testing.T) {
		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// Apply a pgroll migration
			err := m.Start(ctx, &migrations.Migration{
				Name:       "01_initial_migration",
				Operations: migrations.Operations{&migrations.OpRawSQL{Up: "SELECT 1"}},
			}, backfill.NewConfig())
			require.NoError(t, err)
			err = m.Complete(ctx)
			require.NoError(t, err)

			// Get the latest migration name
			latest, err := m.State().LatestMigration(ctx, "public")
			require.NoError(t, err)

			// Get the previous migration name
			previous, err := m.State().PreviousMigration(ctx, "public")
			require.NoError(t, err)

			// We have a latest migration name but no previous migration name
			require.NotNil(t, latest)
			require.Equal(t, "01_initial_migration", *latest)
			require.Nil(t, previous)
		})
	})

	t.Run("two pgroll migrations applied", func(t *testing.T) {
		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// Apply a pgroll migration
			err := m.Start(ctx, &migrations.Migration{
				Name:       "01_initial_migration",
				Operations: migrations.Operations{&migrations.OpRawSQL{Up: "SELECT 1"}},
			}, backfill.NewConfig())
			require.NoError(t, err)
			err = m.Complete(ctx)
			require.NoError(t, err)

			// Apply another pgroll migration
			err = m.Start(ctx, &migrations.Migration{
				Name:       "02_another_migration",
				Operations: migrations.Operations{&migrations.OpRawSQL{Up: "SELECT 1"}},
			}, backfill.NewConfig())
			require.NoError(t, err)
			err = m.Complete(ctx)
			require.NoError(t, err)

			// Get the latest migration name
			latest, err := m.State().LatestMigration(ctx, "public")
			require.NoError(t, err)

			// Get the previous migration name
			previous, err := m.State().PreviousMigration(ctx, "public")
			require.NoError(t, err)

			// We have latest and previous migration names
			require.NotNil(t, latest)
			require.Equal(t, "02_another_migration", *latest)
			require.NotNil(t, previous)
			require.Equal(t, "01_initial_migration", *previous)
		})
	})

	t.Run("one pgroll then one inferred migration", func(t *testing.T) {
		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// Apply a pgroll migration
			err := m.Start(ctx, &migrations.Migration{
				Name:       "01_initial_migration",
				Operations: migrations.Operations{&migrations.OpRawSQL{Up: "SELECT 1"}},
			}, backfill.NewConfig())
			require.NoError(t, err)
			err = m.Complete(ctx)
			require.NoError(t, err)

			// Execute a SQL DDL statement to generate an inferred migration
			_, err = db.ExecContext(ctx, "CREATE TABLE table1(id int)")
			require.NoError(t, err)

			// Get the latest migration name
			latest, err := m.State().LatestMigration(ctx, "public")
			require.NoError(t, err)

			// Get the previous migration name
			previous, err := m.State().PreviousMigration(ctx, "public")
			require.NoError(t, err)

			// We have latest and previous migration names
			require.NotNil(t, latest)
			require.Regexp(t, `01_initial_migration_\d{20}$`, *latest)
			require.NotNil(t, previous)
			require.Equal(t, "01_initial_migration", *previous)
		})
	})
}

func TestLatestAndPreviousVersionMethods(t *testing.T) {
	t.Parallel()

	t.Run("no migrations applied", func(t *testing.T) {
		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// Get the latest version schema name
			latest, err := m.State().LatestVersion(ctx, "public")
			require.NoError(t, err)

			// Get the previous version schema name
			previous, err := m.State().PreviousVersion(ctx, "public")
			require.NoError(t, err)

			// Assert that both latest and previous are nil as no migrations have
			// been applied
			require.Nil(t, latest)
			require.Nil(t, previous)
		})
	})

	t.Run("one inferred migration applied", func(t *testing.T) {
		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// Apply an inferred migration
			_, err := db.ExecContext(ctx, "CREATE TABLE apples(id int)")
			require.NoError(t, err)

			// Get the latest version schema name
			latest, err := m.State().LatestVersion(ctx, "public")
			require.NoError(t, err)

			// Get the previous version schema name
			previous, err := m.State().PreviousVersion(ctx, "public")
			require.NoError(t, err)

			// Assert that both latest and previous are nil as the inferred migration
			// did not create a version schema
			require.Nil(t, latest)
			require.Nil(t, previous)
		})
	})

	t.Run("one pgroll migration applied", func(t *testing.T) {
		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// Apply a pgroll migration
			err := m.Start(ctx, &migrations.Migration{
				Name:          "01_initial_migration",
				VersionSchema: "initial_migration",
				Operations:    migrations.Operations{&migrations.OpRawSQL{Up: "SELECT 1"}},
			}, backfill.NewConfig())
			require.NoError(t, err)
			err = m.Complete(ctx)
			require.NoError(t, err)

			// Get the latest version schema name
			latest, err := m.State().LatestVersion(ctx, "public")
			require.NoError(t, err)

			// Get the previous version schema name
			previous, err := m.State().PreviousVersion(ctx, "public")
			require.NoError(t, err)

			// We have a latest version schema name but no previous one
			require.NotNil(t, latest)
			require.Equal(t, "initial_migration", *latest)
			require.Nil(t, previous)
		})
	})

	t.Run("one pgroll migration applied, another active", func(t *testing.T) {
		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// Apply a pgroll migration
			err := m.Start(ctx, &migrations.Migration{
				Name:          "01_initial_migration",
				VersionSchema: "initial_migration",
				Operations:    migrations.Operations{&migrations.OpRawSQL{Up: "SELECT 1"}},
			}, backfill.NewConfig())
			require.NoError(t, err)
			err = m.Complete(ctx)
			require.NoError(t, err)

			// Start but don't complete another pgroll migration.
			err = m.Start(ctx, &migrations.Migration{
				Name:          "02_another_migration",
				VersionSchema: "another_migration",
				Operations:    migrations.Operations{&migrations.OpRawSQL{Up: "SELECT 1"}},
			}, backfill.NewConfig())
			require.NoError(t, err)

			// Get the latest version schema name
			latest, err := m.State().LatestVersion(ctx, "public")
			require.NoError(t, err)

			// Get the previous version schema name
			previous, err := m.State().PreviousVersion(ctx, "public")
			require.NoError(t, err)

			// We have latest and previous version schema names
			require.NotNil(t, latest)
			require.Equal(t, "another_migration", *latest)
			require.NotNil(t, previous)
			require.Equal(t, "initial_migration", *previous)
		})
	})

	t.Run("two pgroll migrations applied", func(t *testing.T) {
		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// Apply a pgroll migration
			err := m.Start(ctx, &migrations.Migration{
				Name:          "01_initial_migration",
				VersionSchema: "initial_migration",
				Operations:    migrations.Operations{&migrations.OpRawSQL{Up: "SELECT 1"}},
			}, backfill.NewConfig())
			require.NoError(t, err)
			err = m.Complete(ctx)
			require.NoError(t, err)

			// Apply another pgroll migration
			err = m.Start(ctx, &migrations.Migration{
				Name:          "02_another_migration",
				VersionSchema: "another_migration",
				Operations:    migrations.Operations{&migrations.OpRawSQL{Up: "SELECT 1"}},
			}, backfill.NewConfig())
			require.NoError(t, err)
			err = m.Complete(ctx)
			require.NoError(t, err)

			// Get the latest version schema name
			latest, err := m.State().LatestVersion(ctx, "public")
			require.NoError(t, err)

			// Get the previous version schema name
			previous, err := m.State().PreviousVersion(ctx, "public")
			require.NoError(t, err)

			// We have only a latest version schema name because the previous version
			// schema was removed when the second migration was completed
			require.NotNil(t, latest)
			require.Equal(t, "another_migration", *latest)
			require.Nil(t, previous)
		})
	})

	t.Run("one pgroll then one inferred migration", func(t *testing.T) {
		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// Apply a pgroll migration
			err := m.Start(ctx, &migrations.Migration{
				Name:          "01_initial_migration",
				VersionSchema: "initial_migration",
				Operations:    migrations.Operations{&migrations.OpRawSQL{Up: "SELECT 1"}},
			}, backfill.NewConfig())
			require.NoError(t, err)
			err = m.Complete(ctx)
			require.NoError(t, err)

			// Execute a SQL DDL statement to generate an inferred migration
			_, err = db.ExecContext(ctx, "CREATE TABLE table1(id int)")
			require.NoError(t, err)

			// Get the latest version schema name
			latest, err := m.State().LatestVersion(ctx, "public")
			require.NoError(t, err)

			// Get the previous version schema name
			previous, err := m.State().PreviousVersion(ctx, "public")
			require.NoError(t, err)

			// We have a latest version schema name that corresponds to the most
			// recent `pgroll` migration. There is no previous version schema name.
			require.NotNil(t, latest)
			require.Equal(t, "initial_migration", *latest)
			require.Nil(t, previous)
		})
	})

	t.Run("one pgroll, one inferred migration, then another pgroll migration", func(t *testing.T) {
		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// Apply a pgroll migration
			err := m.Start(ctx, &migrations.Migration{
				Name:          "01_initial_migration",
				VersionSchema: "initial_migration",
				Operations:    migrations.Operations{&migrations.OpRawSQL{Up: "SELECT 1"}},
			}, backfill.NewConfig())
			require.NoError(t, err)
			err = m.Complete(ctx)
			require.NoError(t, err)

			// Execute a SQL DDL statement to generate an inferred migration
			_, err = db.ExecContext(ctx, "CREATE TABLE table1(id int)")
			require.NoError(t, err)

			// Start but do not complete a pgroll migration
			err = m.Start(ctx, &migrations.Migration{
				Name:       "02_another_migration",
				Operations: migrations.Operations{&migrations.OpRawSQL{Up: "SELECT 1"}},
			}, backfill.NewConfig())
			require.NoError(t, err)

			// Get the latest version schema name
			latest, err := m.State().LatestVersion(ctx, "public")
			require.NoError(t, err)

			// Get the previous version schema name
			previous, err := m.State().PreviousVersion(ctx, "public")
			require.NoError(t, err)

			// We have a latest and a previous version schema name. The inferred
			// migation that was created after the first pgroll migration is ignored.
			require.NotNil(t, latest)
			require.Equal(t, "02_another_migration", *latest)
			require.NotNil(t, previous)
			require.Equal(t, "initial_migration", *previous)
		})
	})
}
