// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"

	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpAlterColumn)(nil)

func (o *OpAlterColumn) Start(ctx context.Context, conn *sql.DB, stateSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	op := o.innerOperation()

	return op.Start(ctx, conn, stateSchema, tr, s, cbs...)
}

func (o *OpAlterColumn) Complete(ctx context.Context, conn *sql.DB, s *schema.Schema) error {
	op := o.innerOperation()

	return op.Complete(ctx, conn, s)
}

func (o *OpAlterColumn) Rollback(ctx context.Context, conn *sql.DB, tr SQLTransformer) error {
	op := o.innerOperation()

	return op.Rollback(ctx, conn, tr)
}

func (o *OpAlterColumn) Validate(ctx context.Context, s *schema.Schema) error {
	// Ensure that the operation describes only one change to the column
	if cnt := o.numChanges(); cnt != 1 {
		return MultipleAlterColumnChangesError{Changes: cnt}
	}

	// Validate that the table and column exist
	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}
	if table.GetColumn(o.Column) == nil {
		return ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}

	// Ensure that the column has a primary key defined on exactly one column.
	err := checkBackfill(table)
	if err != nil {
		return err
	}

	// Apply any special validation rules for the inner operation
	op := o.innerOperation()
	if _, ok := op.(*OpRenameColumn); ok {
		if o.Up != "" {
			return NoUpSQLAllowedError{}
		}
		if o.Down != "" {
			return NoDownSQLAllowedError{}
		}
	}

	// Validate the inner operation in isolation
	return op.Validate(ctx, s)
}

func (o *OpAlterColumn) innerOperation() Operation {
	switch {
	case o.Name != nil:
		return &OpRenameColumn{
			Table: o.Table,
			From:  o.Column,
			To:    *o.Name,
		}

	case o.Type != nil:
		return &OpChangeType{
			Table:  o.Table,
			Column: o.Column,
			Type:   *o.Type,
			Up:     o.Up,
			Down:   o.Down,
		}

	case o.Check != nil:
		return &OpSetCheckConstraint{
			Table:  o.Table,
			Column: o.Column,
			Check:  *o.Check,
			Up:     o.Up,
			Down:   o.Down,
		}

	case o.References != nil:
		return &OpSetForeignKey{
			Table:      o.Table,
			Column:     o.Column,
			References: *o.References,
			Up:         o.Up,
			Down:       o.Down,
		}

	case o.Nullable != nil && !*o.Nullable:
		return &OpSetNotNull{
			Table:  o.Table,
			Column: o.Column,
			Up:     o.Up,
			Down:   o.Down,
		}

	case o.Nullable != nil && *o.Nullable:
		return &OpDropNotNull{
			Table:  o.Table,
			Column: o.Column,
			Up:     o.Up,
			Down:   o.Down,
		}

	case o.Unique != nil:
		return &OpSetUnique{
			Table:  o.Table,
			Column: o.Column,
			Name:   o.Unique.Name,
			Up:     o.Up,
			Down:   o.Down,
		}
	}
	return nil
}

// numChanges returns the number of kinds of change that one 'alter column'
// operation represents.
func (o *OpAlterColumn) numChanges() int {
	fieldsSet := 0

	if o.Name != nil {
		fieldsSet++
	}
	if o.Type != nil {
		fieldsSet++
	}
	if o.Check != nil {
		fieldsSet++
	}
	if o.References != nil {
		fieldsSet++
	}
	if o.Nullable != nil {
		fieldsSet++
	}
	if o.Unique != nil {
		fieldsSet++
	}

	return fieldsSet
}
