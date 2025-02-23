// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpRawSQL)(nil)

func (o *OpRawSQL) Start(ctx context.Context, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	if o.OnComplete {
		return nil, nil
	}

	_, err := conn.ExecContext(ctx, o.Up)
	return nil, err
}

func (o *OpRawSQL) Complete(ctx context.Context, conn db.DB, s *schema.Schema) error {
	if !o.OnComplete {
		return nil
	}

	_, err := conn.ExecContext(ctx, o.Up)
	return err
}

func (o *OpRawSQL) Rollback(ctx context.Context, conn db.DB, s *schema.Schema) error {
	if o.Down == "" {
		return nil
	}

	_, err := conn.ExecContext(ctx, o.Down)
	return err
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
