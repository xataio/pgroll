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

	switch o.Type {
	case OpCreateConstraintTypeCheck:
		return table, o.addCheckConstraint(ctx, conn)
	case OpCreateConstraintTypeUnique:
		return table, o.addUniqueConstraint(ctx, conn)
	case OpCreateConstraintTypeForeignKey:
		return table, o.addForeignKeyConstraint(ctx, conn)
	}

	return table, nil
}

func (o *OpCreateConstraint) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	switch o.Type {
	case OpCreateConstraintTypeCheck:
		checkOp := &OpSetCheckConstraint{
			Table: o.Table,
			Check: CheckConstraint{
				Name: o.Name,
			},
		}
		return checkOp.Complete(ctx, conn, tr, s)
	case OpCreateConstraintTypeUnique:
		uniqueOp := &OpSetUnique{
			Table: o.Table,
			Name:  o.Name,
		}
		return uniqueOp.Complete(ctx, conn, tr, s)
	case OpCreateConstraintTypeForeignKey:
		fkOp := &OpSetForeignKey{
			Table: o.Table,
			References: ForeignKeyReference{
				Name: o.Name,
			},
		}
		return fkOp.Complete(ctx, conn, tr, s)
	}

	return nil
}

func (o *OpCreateConstraint) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer) error {
	return nil
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

	switch o.Type {
	case OpCreateConstraintTypeCheck:
		if o.Check == nil || *o.Check == "" {
			return FieldRequiredError{Name: "check"}
		}
	case OpCreateConstraintTypeUnique:
		if len(o.Columns) == 0 {
			return FieldRequiredError{Name: "columns"}
		}
	case OpCreateConstraintTypeForeignKey:
		if o.References == nil {
			return FieldRequiredError{Name: "references"}
		}
		if err := o.References.Validate(s); err != nil {
			return err
		}
	}

	return nil
}

func (o *OpCreateConstraint) addCheckConstraint(ctx context.Context, conn db.DB) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s) NOT VALID",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.Name),
		rewriteCheckExpression(*o.Check, o.Columns...),
	))

	return err
}

func (o *OpCreateConstraint) addUniqueConstraint(ctx context.Context, conn db.DB) error {
	cols := make([]string, len(o.Columns))
	for i, col := range o.Columns {
		cols[i] = pq.QuoteIdentifier(TemporaryName(col))
	}
	_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)",
		pq.QuoteIdentifier(o.Name),
		pq.QuoteIdentifier(o.Table),
		strings.Join(cols, ", "),
	))

	return err
}

func (o *OpCreateConstraint) addForeignKeyConstraint(ctx context.Context, conn db.DB) error {
	cols := make([]string, len(o.Columns))
	for i, col := range o.Columns {
		cols[i] = pq.QuoteIdentifier(TemporaryName(col))
	}

	onDelete := string(ForeignKeyReferenceOnDeleteNOACTION)
	if o.References.OnDelete != "" {
		onDelete = strings.ToUpper(string(o.References.OnDelete))
	}

	var references string
	if o.References.Column != nil {
		references = fmt.Sprintf("%s (%s)", pq.QuoteIdentifier(o.References.Table), pq.QuoteIdentifier(*o.References.Column))
	} else if o.References.Columns != nil {
		refCols := make([]string, len(o.Columns))
		for i, col := range o.References.Columns {
			cols[i] = pq.QuoteIdentifier(TemporaryName(col))
		}
		references = fmt.Sprintf("%s (%s)", pq.QuoteIdentifier(o.References.Table), strings.Join(refCols, ", "))
	}

	_, err := conn.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s ON DELETE %s NOT VALID",
			pq.QuoteIdentifier(o.Table),
			pq.QuoteIdentifier(o.Name),
			strings.Join(cols, ", "),
			references,
			onDelete,
		))

	return err
}
