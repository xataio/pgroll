// SPDX-License-Identifier: Apache-2.0

package db

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/cloudflare/backoff"
	"github.com/lib/pq"
)

const (
	lockNotAvailableErrorCode pq.ErrorCode = "55P03"
	maxBackoffDuration                     = 1 * time.Minute
	backoffInterval                        = 1 * time.Second
)

type DB interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	WithRetryableTransaction(ctx context.Context, f func(context.Context, *sql.Tx) error) error
	Close() error
}

// RDB wraps a *sql.DB and retries queries using an exponential backoff (with
// jitter) on lock_timeout errors.
type RDB struct {
	DB *sql.DB
}

// ExecContext wraps sql.DB.ExecContext, retrying queries on lock_timeout errors.
func (db *RDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	b := backoff.New(maxBackoffDuration, backoffInterval)

	for {
		res, err := db.DB.ExecContext(ctx, query, args...)
		if err == nil {
			return res, nil
		}

		pqErr := &pq.Error{}
		if errors.As(err, &pqErr) && pqErr.Code == lockNotAvailableErrorCode {
			if err := sleepCtx(ctx, b.Duration()); err != nil {
				return nil, err
			}
			continue
		}

		return nil, err
	}
}

// QueryContext wraps sql.DB.QueryContext, retrying queries on lock_timeout errors.
func (db *RDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	b := backoff.New(maxBackoffDuration, backoffInterval)

	for {
		rows, err := db.DB.QueryContext(ctx, query, args...)
		if err == nil {
			return rows, nil
		}

		pqErr := &pq.Error{}
		if errors.As(err, &pqErr) && pqErr.Code == lockNotAvailableErrorCode {
			if err := sleepCtx(ctx, b.Duration()); err != nil {
				return nil, err
			}
			continue
		}

		return nil, err
	}
}

// WithRetryableTransaction runs `f` in a transaction, retrying on lock_timeout errors.
func (db *RDB) WithRetryableTransaction(ctx context.Context, f func(context.Context, *sql.Tx) error) error {
	b := backoff.New(maxBackoffDuration, backoffInterval)

	for {
		tx, err := db.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		err = f(ctx, tx)
		if err == nil {
			return tx.Commit()
		}

		if errRollback := tx.Rollback(); errRollback != nil {
			return errRollback
		}

		pqErr := &pq.Error{}
		if errors.As(err, &pqErr) && pqErr.Code == lockNotAvailableErrorCode {
			if err := sleepCtx(ctx, b.Duration()); err != nil {
				return err
			}
			continue
		}

		return err
	}
}

func (db *RDB) Close() error {
	return db.DB.Close()
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// ScanFirstValue is a helper function to scan the first value with the assumption that Rows contains
// a single row with a single value.
func ScanFirstValue[T any](rows *sql.Rows, dest *T) error {
	if rows.Next() {
		if err := rows.Scan(dest); err != nil {
			return err
		}
	}
	return rows.Err()
}
