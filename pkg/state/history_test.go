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

		// Ensure that the schema history is correct
		res, err := state.SchemaHistory(ctx, "public")
		require.NoError(t, err)

		assert.Equal(t, 2, len(res))
		assert.Equal(t, migs[0].Name, res[0].Migration.Name)
		assert.Equal(t, migs[1].Name, res[1].Migration.Name)

		assert.Equal(t, migs[0].Operations, res[0].Migration.Operations)
		assert.Equal(t, migs[1].Operations, res[1].Migration.Operations)
	})
}

func ptr[T any](v T) *T {
	return &v
}
