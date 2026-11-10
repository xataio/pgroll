// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/state"
)

func TestMain(m *testing.M) {
	testutils.SharedTestMain(m)
}

func TestMigrateFailsWhenMigrationIsActive(t *testing.T) {
	testutils.WithConnectionToContainer(t, func(_ *sql.DB, connStr string) {
		ctx := context.Background()

		viper.Set("PG_URL", connStr)
		viper.Set("SCHEMA", "public")
		viper.Set("STATE_SCHEMA", "pgroll")
		viper.Set("LOCK_TIMEOUT", 500)
		t.Cleanup(viper.Reset)

		st, err := state.New(ctx, connStr, "pgroll")
		require.NoError(t, err)
		require.NoError(t, st.Init(ctx))

		dir := t.TempDir()
		err = os.WriteFile(filepath.Join(dir, "01_create_table.yaml"), []byte(`operations:
  - create_table:
      name: items
      columns:
        - name: id
          type: integer
          pk: true
`), 0o600)
		require.NoError(t, err)

		// The first run leaves the migration active
		require.NoError(t, runMigrate(ctx, dir))

		// A second run must fail while that migration is still active
		err = runMigrate(ctx, dir)
		require.ErrorContains(t, err, `migration "01_create_table" is active`)
	})
}

func runMigrate(ctx context.Context, dir string) error {
	cmd := migrateCmd()
	cmd.SetArgs([]string{dir})
	return cmd.ExecuteContext(ctx)
}
