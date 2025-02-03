// SPDX-License-Identifier: Apache-2.0

package db_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/db"
)

func TestMain(m *testing.M) {
	testutils.SharedTestMain(m)
}

func TestExecContext(t *testing.T) {
	t.Parallel()

	testutils.WithConnectionToContainer(t, func(conn *sql.DB, connStr string) {
		ctx := context.Background()
		// create a table on which an exclusive lock is held for 2 seconds
		setupTableLock(t, connStr, 2*time.Second)

		// set the lock timeout to 100ms
		ensureLockTimeout(t, conn, 100)

		// execute a query that should retry until the lock is released
		rdb := &db.RDB{DB: conn}
		_, err := rdb.ExecContext(ctx, "INSERT INTO test(id) VALUES (1)")
		require.NoError(t, err)
	})
}

func TestExecContextWhenContextCancelled(t *testing.T) {
	t.Parallel()

	testutils.WithConnectionToContainer(t, func(conn *sql.DB, connStr string) {
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)

		// create a table on which an exclusive lock is held for 2 seconds
		setupTableLock(t, connStr, 2*time.Second)

		// set the lock timeout to 100ms
		ensureLockTimeout(t, conn, 100)

		// execute a query that should retry until the lock is released
		rdb := &db.RDB{DB: conn}

		// Cancel the context before the lock times out
		go time.AfterFunc(500*time.Millisecond, cancel)

		_, err := rdb.ExecContext(ctx, "INSERT INTO test(id) VALUES (1)")
		require.Errorf(t, err, "context canceled")
	})
}

func TestQueryContext(t *testing.T) {
	t.Parallel()

	testutils.WithConnectionToContainer(t, func(conn *sql.DB, connStr string) {
		ctx := context.Background()
		// create a table on which an exclusive lock is held for 2 seconds
		setupTableLock(t, connStr, 2*time.Second)

		// set the lock timeout to 100ms
		ensureLockTimeout(t, conn, 100)

		// execute a query that should retry until the lock is released
		rdb := &db.RDB{DB: conn}
		rows, err := rdb.QueryContext(ctx, "SELECT COUNT(*) FROM test")
		require.NoError(t, err)

		var count int
		err = db.ScanFirstValue(rows, &count)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}

func TestQueryContextWhenContextCancelled(t *testing.T) {
	t.Parallel()

	testutils.WithConnectionToContainer(t, func(conn *sql.DB, connStr string) {
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)

		// create a table on which an exclusive lock is held for 2 seconds
		setupTableLock(t, connStr, 2*time.Second)

		// set the lock timeout to 100ms
		ensureLockTimeout(t, conn, 100)

		// execute a query that should retry until the lock is released
		rdb := &db.RDB{DB: conn}

		// Cancel the context before the lock times out
		go time.AfterFunc(500*time.Millisecond, cancel)

		_, err := rdb.QueryContext(ctx, "SELECT COUNT(*) FROM test")
		require.Errorf(t, err, "context canceled")
	})
}

func TestWithRetryableTransaction(t *testing.T) {
	t.Parallel()

	testutils.WithConnectionToContainer(t, func(conn *sql.DB, connStr string) {
		ctx := context.Background()

		// create a table on which an exclusive lock is held for 2 seconds
		setupTableLock(t, connStr, 2*time.Second)

		// set the lock timeout to 100ms
		ensureLockTimeout(t, conn, 100)

		// run a transaction that should retry until the lock is released
		rdb := &db.RDB{DB: conn}
		err := rdb.WithRetryableTransaction(ctx, nil, func(ctx context.Context, tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, "SELECT 1 FROM test").Err()
		})
		require.NoError(t, err)
	})
}

func TestWithRetryableTransactionWhenContextCancelled(t *testing.T) {
	t.Parallel()

	testutils.WithConnectionToContainer(t, func(conn *sql.DB, connStr string) {
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)

		// create a table on which an exclusive lock is held for 2 seconds
		setupTableLock(t, connStr, 2*time.Second)

		// set the lock timeout to 100ms
		ensureLockTimeout(t, conn, 100)

		// run a transaction that should retry until the lock is released
		rdb := &db.RDB{DB: conn}

		// Cancel the context before the lock times out
		go time.AfterFunc(500*time.Millisecond, cancel)

		err := rdb.WithRetryableTransaction(ctx, nil, func(ctx context.Context, tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, "SELECT 1 FROM test").Err()
		})
		require.Errorf(t, err, "context canceled")
	})
}

// setupTableLock:
// * connects to the database
// * creates a table in the database
// * starts a transaction that temporarily locks the table
func setupTableLock(t *testing.T, connStr string, d time.Duration) {
	t.Helper()
	ctx := context.Background()

	// connect to the database
	conn2, err := sql.Open("postgres", connStr)
	require.NoError(t, err)

	// create a table in the database
	_, err = conn2.ExecContext(ctx, "CREATE TABLE test (id INT PRIMARY KEY)")
	require.NoError(t, err)

	// start a transaction that takes a temporary lock on the table
	errCh := make(chan error)
	go func() {
		// begin a transaction
		tx, err := conn2.Begin()
		if err != nil {
			errCh <- err
			return
		}

		// lock the table
		_, err = tx.ExecContext(ctx, "LOCK TABLE test IN ACCESS EXCLUSIVE MODE")
		if err != nil {
			errCh <- err
			return
		}

		// signal that the lock is obtained
		errCh <- nil

		// temporarily hold the lock
		time.Sleep(d)

		// commit the transaction
		tx.Commit()
	}()

	// wait for the lock to be obtained
	err = <-errCh
	require.NoError(t, err)
}

func ensureLockTimeout(t *testing.T, conn *sql.DB, ms int) {
	t.Helper()

	// Set the lock timeout
	query := fmt.Sprintf("SET lock_timeout = '%dms'", ms)
	_, err := conn.ExecContext(context.Background(), query)
	require.NoError(t, err)

	// Ensure the lock timeout is set
	var lockTimeout string
	err = conn.QueryRowContext(context.Background(), "SHOW lock_timeout").Scan(&lockTimeout)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%dms", ms), lockTimeout)
}
