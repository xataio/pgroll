// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

// Please note that this is different from raw SQL operations. These migration
// files only contain SQL and the statements are run in transactions just like
// in other migration tools.

func (o *OpSQLInTransaction) Start(ctx context.Context, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	err := conn.WithRetryableTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, o.Up)
		return err
	})
	return nil, err
}

// Complete is a no-op.
func (o *OpSQLInTransaction) Complete(ctx context.Context, conn db.DB, s *schema.Schema) error {
	return nil
}

func (o *OpSQLInTransaction) Rollback(ctx context.Context, conn db.DB, s *schema.Schema) error {
	if o.Down == "" {
		return nil
	}

	return conn.WithRetryableTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, o.Down)
		return err
	})
}

func (o *OpSQLInTransaction) Validate(ctx context.Context, s *schema.Schema) error {
	if o.Up == "" {
		return EmptyMigrationError{}
	}

	return nil
}

func (o *OpSQLInTransaction) RequiresSchemaRefresh() {}

func (o *OpSQLInTransaction) IsIsolated() bool { return true }
