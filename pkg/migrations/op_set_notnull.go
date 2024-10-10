// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

type OpSetNotNull struct {
	Table  string `json:"table"`
	Column string `json:"column"`
	Up     string `json:"up"`
	Down   string `json:"down"`
}

var _ Operation = (*OpSetNotNull)(nil)

func (o *OpSetNotNull) Start(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	table := s.GetTable(o.Table)

	// Add an unchecked NOT NULL constraint to the new column.
	if err := addNotNullConstraint(ctx, conn, o.Table, o.Column, TemporaryName(o.Column)); err != nil {
		return nil, fmt.Errorf("failed to add not null constraint: %w", err)
	}

	return table, nil
}

func (o *OpSetNotNull) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	// Validate the NOT NULL constraint on the old column.
	// The constraint must be valid because:
	// * Existing NULL values in the old column were rewritten using the `up` SQL during backfill.
	// * New NULL values written to the old column during the migration period were also rewritten using `up` SQL.
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s VALIDATE CONSTRAINT %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(NotNullConstraintName(o.Column))))
	if err != nil {
		return err
	}

	// Use the validated constraint to add `NOT NULL` to the new column
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s ALTER COLUMN %s SET NOT NULL",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(TemporaryName(o.Column))))
	if err != nil {
		return err
	}

	// Drop the NOT NULL constraint
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s DROP CONSTRAINT IF EXISTS %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(NotNullConstraintName(o.Column))))
	if err != nil {
		return err
	}

	return nil
}

func (o *OpSetNotNull) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer) error {
	return nil
}

func (o *OpSetNotNull) Validate(ctx context.Context, s *schema.Schema) error {
	column := s.GetTable(o.Table).GetColumn(o.Column)

	if !column.Nullable {
		return ColumnIsNotNullableError{Table: o.Table, Name: o.Column}
	}

	if o.Up == "" {
		return FieldRequiredError{Name: "up"}
	}

	return nil
}
