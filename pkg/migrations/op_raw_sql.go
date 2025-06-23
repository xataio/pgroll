// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var (
	_ Operation  = (*OpRawSQL)(nil)
	_ Createable = (*OpRawSQL)(nil)
)

func (o *OpRawSQL) Start(ctx context.Context, l Logger, conn db.DB, latestSchema string, s *schema.Schema) (*backfill.Task, error) {
	l.LogOperationStart(o)

	if o.OnComplete {
		return nil, nil
	}

	return nil, NewRawSQLAction(conn, o.Up).Execute(ctx)
}

func (o *OpRawSQL) Complete(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationComplete(o)

	if !o.OnComplete {
		return []DBAction{}, nil
	}

	return []DBAction{NewRawSQLAction(conn, o.Up)}, nil
}

func (o *OpRawSQL) Rollback(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationRollback(o)

	if o.Down == "" {
		return []DBAction{}, nil
	}

	return []DBAction{NewRawSQLAction(conn, o.Down)}, nil
}

func (o *OpRawSQL) Validate(ctx context.Context, s *schema.Schema) error {
	if o.Up == "" {
		return EmptyMigrationError{}
	}

	if o.OnComplete && o.Down != "" {
		return InvalidMigrationError{Reason: "down is not allowed with onComplete"}
	}

	return nil
}

// IsIsolated returns true if the operation is isolated and should be run with other operations.
func (o *OpRawSQL) IsIsolated() bool {
	return !o.OnComplete
}

func (o *OpRawSQL) RequiresSchemaRefresh() {}
