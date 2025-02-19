// SPDX-License-Identifier: Apache-2.0

package defaults_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/roll"

	"github.com/xataio/pgroll/internal/defaults"
	"github.com/xataio/pgroll/internal/testutils"
)

func TestMain(m *testing.M) {
	testutils.SharedTestMain(m)
}

func TestFastPathDefaults(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name             string
		ColumnType       string
		Default          string
		ExpectedFastPath bool
	}{
		{
			Name:             "constant integer",
			ColumnType:       "int",
			Default:          "10",
			ExpectedFastPath: true,
		},
		{
			Name:             "constant boolean",
			ColumnType:       "bool",
			Default:          "true",
			ExpectedFastPath: true,
		},
		{
			Name:             "simple arithmetic",
			ColumnType:       "int",
			Default:          "1 + 2 + 3",
			ExpectedFastPath: true,
		},
		{
			Name:             "random() function",
			ColumnType:       "double precision",
			Default:          "random()",
			ExpectedFastPath: false,
		},
		{
			Name:             "random() function with typecast",
			ColumnType:       "integer",
			Default:          "(random()*1000)::integer",
			ExpectedFastPath: false,
		},
		{
			Name:             "timeofday() function",
			ColumnType:       "text",
			Default:          "timeofday()",
			ExpectedFastPath: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			testutils.WithMigratorAndConnectionToContainer(t, func(mig *roll.Roll, conn *sql.DB) {
				ctx := context.Background()
				rdb := &db.RDB{DB: conn}

				createTestTable(t, conn)

				fp, err := defaults.UsesFastPath(ctx, rdb, "test_table", tc.ColumnType, tc.Default)
				require.NoError(t, err)

				require.Equal(t, tc.ExpectedFastPath, fp)
			})
		})
	}
}

func createTestTable(t *testing.T, conn *sql.DB) {
	t.Helper()

	_, err := conn.Exec(`
    CREATE TABLE test_table (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL
    )
  `)
	require.NoError(t, err)
}
