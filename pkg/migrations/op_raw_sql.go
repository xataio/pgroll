// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"

	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpRawSQL)(nil)

func (o *OpRawSQL) Start(ctx context.Context, conn *sql.DB, stateSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	if !o.OnComplete {
		_, err := conn.ExecContext(ctx, o.Up)
		return nil, err
	}
	return nil, nil
}

func (o *OpRawSQL) Complete(ctx context.Context, conn *sql.DB, tr SQLTransformer, s *schema.Schema) error {
	if o.OnComplete {
		_, err := conn.ExecContext(ctx, o.Up)
		return err
	}
	return nil
}

func (o *OpRawSQL) Rollback(ctx context.Context, conn *sql.DB, tr SQLTransformer) error {
	if o.Down != "" {
		_, err := conn.ExecContext(ctx, o.Down)
		return err
	}
	return nil
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

func (o *OpRawSQL) IsIsolated() bool {
	return !o.OnComplete
}

func (o *OpRawSQL) RequiresSchemaRefresh() {}
