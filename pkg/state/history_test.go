// SPDX-License-Identifier: Apache-2.0

package state_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/state"
)

func TestSchemaHistoryReturnsFullSchemaHistory(t *testing.T) {
	t.Parallel()

	testutils.WithStateAndConnectionToContainer(t, func(state *state.State, db *sql.DB) {
		ctx := context.Background()
		migs := []migrations.Migration{
			{
				Name: "01_add_table",
				Operations: migrations.Operations{
					&migrations.OpCreateTable{
						Name: "users",
						Columns: []migrations.Column{
							{
								Name: "id",
								Type: "serial",
								Pk:   true,
							},
							{
								Name:     "username",
								Type:     "text",
								Nullable: false,
							},
						},
					},
				},
			},
			{
				Name: "02_set_nullable",
				Operations: migrations.Operations{
					&migrations.OpAlterColumn{
						Table:    "users",
						Column:   "username",
						Nullable: ptr(false),
						Up:       "username",
					},
				},
			},
		}

		// Start and complete both migrations
		for _, mig := range migs {
			_, err := state.Start(ctx, "public", &mig)
			require.NoError(t, err)
			err = state.Complete(ctx, "public", mig.Name)
			require.NoError(t, err)
		}

		// Get the schema history
		res, err := state.SchemaHistory(ctx, "public")
		require.NoError(t, err)

		// Parse the raw migrations from the schema history into actual migrations
		actualMigs := make([]migrations.Migration, len(migs))
		for i := range res {
			m, err := migrations.ParseMigration(&res[i].Migration)
			require.NoError(t, err)
			actualMigs[i] = *m
		}

		// Assert that the schema history is correct
		assert.Equal(t, 2, len(res))
		assert.Equal(t, migs, actualMigs)
	})
}

func TestSchemaHistoryDoesNotReturnBaselineMigrations(t *testing.T) {
	t.Parallel()

	t.Run("baseline migration does not appear in schema history", func(t *testing.T) {
		testutils.WithStateAndConnectionToContainer(t, func(state *state.State, db *sql.DB) {
			ctx := context.Background()

			// Create a baseline migration
			err := state.CreateBaseline(ctx, "public", "01_initial_version")
			require.NoError(t, err)

			// Get the schema history
			res, err := state.SchemaHistory(ctx, "public")
			require.NoError(t, err)

			// Assert that the schema history is empty
			assert.Equal(t, 0, len(res))
		})
	})

	t.Run("migrations before the most recent baseline do not appear in the schema history", func(t *testing.T) {
		testutils.WithStateAndConnectionToContainer(t, func(state *state.State, db *sql.DB) {
			ctx := context.Background()

			// Execute DDL to create an inferred migration
			_, err := db.ExecContext(ctx, "CREATE TABLE users (id int)")
			require.NoError(t, err)

			// Create a baseline migration
			err = state.CreateBaseline(ctx, "public", "01_initial_version")
			require.NoError(t, err)

			// Get the schema history
			res, err := state.SchemaHistory(ctx, "public")
			require.NoError(t, err)

			// Assert that the schema history is empty
			assert.Equal(t, 0, len(res))
		})
	})

	t.Run("migrations after the most recent baseline are included in the history", func(t *testing.T) {
		testutils.WithStateAndConnectionToContainer(t, func(state *state.State, db *sql.DB) {
			ctx := context.Background()

			// Execute DDL to create an inferred migration
			_, err := db.ExecContext(ctx, "CREATE TABLE users (id int)")
			require.NoError(t, err)

			// Create a baseline migration
			err = state.CreateBaseline(ctx, "public", "01_initial_version")
			require.NoError(t, err)

			// Execute DDL to create another inferred migration
			_, err = db.ExecContext(ctx, "CREATE TABLE fruits (id int)")
			require.NoError(t, err)

			// Get the schema history
			res, err := state.SchemaHistory(ctx, "public")
			require.NoError(t, err)

			// Assert that one migration is present in the schema history
			require.Equal(t, 1, len(res))

			// Deserialize the migration from the history.
			mig, err := migrations.ParseMigration(&res[0].Migration)
			require.NoError(t, err)

			// Assert that the migration is the one that was created after the baseline
			expectedOperations := migrations.Operations{
				&migrations.OpRawSQL{
					Up: "CREATE TABLE fruits (id int)",
				},
			}
			assert.Equal(t, expectedOperations, mig.Operations)
		})
	})
}

func ptr[T any](v T) *T {
	return &v
}
