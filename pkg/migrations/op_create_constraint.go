// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"
	"strings"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpCreateConstraint)(nil)

func (o *OpCreateConstraint) Start(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	table := s.GetTable(o.Table)

	switch o.Type { //nolint:gocritic // more cases will be added
	case OpCreateConstraintTypeUnique:
		return table, o.addUniqueConstraint(ctx, conn)
	}

	return table, nil
}

func (o *OpCreateConstraint) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	switch o.Type { //nolint:gocritic // more cases will be added
	case OpCreateConstraintTypeUnique:
		uniqueOp := &OpSetUnique{
			Table: o.Table,
			Name:  o.Name,
		}
		return uniqueOp.Complete(ctx, conn, tr, s)
	}

	return nil
}

func (o *OpCreateConstraint) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	var err error
	switch o.Type { //nolint:gocritic // more cases will be added
	case OpCreateConstraintTypeUnique:
		_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP INDEX CONCURRENTLY IF EXISTS %s",
			pq.QuoteIdentifier(o.Name)))
	}

	return err
}

func (o *OpCreateConstraint) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	if err := ValidateIdentifierLength(o.Name); err != nil {
		return err
	}

	if table.ConstraintExists(o.Name) {
		return ConstraintAlreadyExistsError{
			Table:      o.Table,
			Constraint: o.Name,
		}
	}

	for _, col := range o.Columns {
		if table.GetColumn(col) == nil {
			return ColumnDoesNotExistError{
				Table: o.Table,
				Name:  col,
			}
		}
	}

	switch o.Type { //nolint:gocritic // more cases will be added
	case OpCreateConstraintTypeUnique:
		if len(o.Columns) == 0 {
			return FieldRequiredError{Name: "columns"}
		}
	}

	return nil
}

func (o *OpCreateConstraint) addUniqueIndex(ctx context.Context, conn db.DB) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)",
		pq.QuoteIdentifier(o.Name),
		pq.QuoteIdentifier(o.Table),
		strings.Join(quoteColumnNames(o.Columns), ", "),
	))

	return err
}
