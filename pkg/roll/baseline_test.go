// SPDX-License-Identifier: Apache-2.0

package roll_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/roll"
	"github.com/xataio/pgroll/pkg/schema"
	"github.com/xataio/pgroll/pkg/state"
)

func TestBaseline(t *testing.T) {
	t.Parallel()

	t.Run("baseline migration captures the current schema", func(t *testing.T) {
		testutils.WithMigratorAndStateAndConnectionToContainerWithOptions(t, nil, func(roll *roll.Roll, st *state.State, db *sql.DB) {
			ctx := context.Background()

			// Create a table in the database to simulate an existing schema
			_, err := db.ExecContext(ctx, "CREATE TABLE users (id int)")
			require.NoError(t, err)

			// Create a baseline migration
			err = roll.CreateBaseline(ctx, "01_initial_version")
			require.NoError(t, err)

			// Get the captured database schema after the baseline migration was applied
			sc, err := st.SchemaAfterMigration(ctx, "public", "01_initial_version")
			require.NoError(t, err)

			// Define the expected schema
			wantSchema := &schema.Schema{
				Name: "public",
				Tables: map[string]*schema.Table{
					"users": {
						Name: "users",
						Columns: map[string]*schema.Column{
							"id": {
								Name:         "id",
								Type:         "integer",
								Nullable:     true,
								PostgresType: "base",
							},
						},
					},
				},
			}

			// Clear OIDs from the schema to avoid comparison issues
			clearOIDS(sc)

			// Assert the the schema matches the expected schema
			require.Equal(t, wantSchema, sc)
		})
	})
}

func clearOIDS(s *schema.Schema) {
	for k := range s.Tables {
		c := s.Tables[k]
		c.OID = ""
		s.Tables[k] = c
	}
}
