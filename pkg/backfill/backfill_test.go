// SPDX-License-Identifier: Apache-2.0

package backfill_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
)

func TestMain(m *testing.M) {
	testutils.SharedTestMain(m)
}

func TestGetRowCountDoesNotLeakConnections(t *testing.T) {
	t.Parallel()

	testutils.WithConnectionToContainer(t, func(conn *sql.DB, connStr string) {
		// Use a short timeout so that if connections are leaked and the pool is
		// exhausted, the test fails quickly instead of hanging.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		tableName := "test_row_count"
		_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", tableName))
		require.NoError(t, err)

		// Call getRowCount many times with a small pool to verify no connection leak.
		// With only 5 max connections, leaked connections would quickly cause errors.
		conn2, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer conn2.Close()

		conn2.SetMaxOpenConns(5)
		rdb := &db.RDB{DB: conn2}

		for i := 0; i < 20; i++ {
			_, err := backfill.GetRowCount(ctx, rdb, tableName)
			require.NoError(t, err, "iteration %d: getRowCount should not fail due to connection leak", i)
		}
	})
}
