// SPDX-License-Identifier: Apache-2.0

package db

import (
	"context"
	"database/sql"
)

// FakeDB is a fake implementation of `DB`. All methods on `FakeDB` are
// implemented as no-ops
type FakeDB struct{}

func (db *FakeDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return nil, nil
}

func (db *FakeDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return nil, nil
}

func (db *FakeDB) WithRetryableTransaction(ctx context.Context, f func(context.Context, *sql.Tx) error) error {
	return nil
}

func (db *FakeDB) RawConn() *sql.DB {
	return nil
}

func (db *FakeDB) Close() error {
	return nil
}
