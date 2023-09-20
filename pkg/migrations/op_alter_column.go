package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/xataio/pg-roll/pkg/schema"
)

type OpAlterColumn struct {
	Table      string               `json:"table"`
	Column     string               `json:"column"`
	Name       string               `json:"name"`
	Type       string               `json:"type"`
	Check      *CheckConstraint     `json:"check"`
	References *ForeignKeyReference `json:"references"`
	NotNull    *bool                `json:"not_null"`
	Unique     *UniqueConstraint    `json:"unique"`
	Up         string               `json:"up"`
	Down       string               `json:"down"`
}

var _ Operation = (*OpAlterColumn)(nil)

func (o *OpAlterColumn) Start(ctx context.Context, conn *sql.DB, stateSchema string, s *schema.Schema) error {
	op := o.innerOperation()

	return op.Start(ctx, conn, stateSchema, s)
}

func (o *OpAlterColumn) Complete(ctx context.Context, conn *sql.DB) error {
	op := o.innerOperation()

	return op.Complete(ctx, conn)
}

func (o *OpAlterColumn) Rollback(ctx context.Context, conn *sql.DB) error {
	op := o.innerOperation()

	return op.Rollback(ctx, conn)
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

	// Apply any special validation rules for the inner operation
	op := o.innerOperation()
	switch op.(type) {
	case *OpRenameColumn, *OpSetUnique:
		if o.Up != "" {
			return NoUpSQLAllowedError{}
		}
		if o.Down != "" {
			return NoDownSQLAllowedError{}
		}

	case *OpSetNotNull:
		if o.NotNull != nil && !*o.NotNull {
			return fmt.Errorf("removing NOT NULL constraints is not supported")
		}
	}

	// Validate the inner operation in isolation
	return op.Validate(ctx, s)
}

func (o *OpAlterColumn) innerOperation() Operation {
	switch {
	case o.Name != "":
		return &OpRenameColumn{
			Table: o.Table,
			From:  o.Column,
			To:    o.Name,
		}

	case o.Type != "":
		return &OpChangeType{
			Table:  o.Table,
			Column: o.Column,
			Type:   o.Type,
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

	case o.NotNull != nil:
		return &OpSetNotNull{
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
		}
	}
	return nil
}

// numChanges returns the number of kinds of change that one 'alter column'
// operation represents.
func (o *OpAlterColumn) numChanges() int {
	fieldsSet := 0

	if o.Name != "" {
		fieldsSet++
	}
	if o.Type != "" {
		fieldsSet++
	}
	if o.Check != nil {
		fieldsSet++
	}
	if o.References != nil {
		fieldsSet++
	}
	if o.NotNull != nil {
		fieldsSet++
	}
	if o.Unique != nil {
		fieldsSet++
	}

	return fieldsSet
}
