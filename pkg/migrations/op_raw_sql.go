// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"

	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpRawSQL)(nil)

func (o *OpRawSQL) Start(ctx context.Context, conn *sql.DB, stateSchema string, s *schema.Schema, cbs ...CallbackFn) error {
	if !o.onComplete() {
		_, err := conn.ExecContext(ctx, o.Up)
		return err
	}
	return nil
}

func (o *OpRawSQL) Complete(ctx context.Context, conn *sql.DB, s *schema.Schema) error {
	if o.onComplete() {
		_, err := conn.ExecContext(ctx, o.Up)
		return err
	}
	return nil
}

func (o *OpRawSQL) Rollback(ctx context.Context, conn *sql.DB) error {
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

	if o.onComplete() && o.Down != "" {
		return InvalidMigrationError{Reason: "down is not allowed with onComplete"}
	}

	return nil
}

func (o *OpRawSQL) onComplete() bool {
	if o.OnComplete != nil {
		return *o.OnComplete
	}
	return false
}

// this operation is isolated when executed on start, cannot be executed with other operations
func (o *OpRawSQL) IsIsolated() bool {
	return !o.onComplete()
}

// this operation requires the resulting schema to be refreshed when executed on start
func (o *OpRawSQL) RequiresSchemaRefresh() bool {
	return !o.onComplete()
}
