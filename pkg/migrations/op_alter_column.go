package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/xataio/pg-roll/pkg/schema"
)

type OpAlterColumn struct {
	Table      string           `json:"table"`
	Column     string           `json:"column"`
	Name       string           `json:"name"`
	Type       string           `json:"type"`
	Check      string           `json:"check"`
	References *ColumnReference `json:"references"`
	NotNull    *bool            `json:"not_null"`
	Up         string           `json:"up"`
	Down       string           `json:"down"`
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
	if !o.oneChange() {
		return MultipleAlterColumnChangesError{}
	}

	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	if table.GetColumn(o.Column) == nil {
		return ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}

	op := o.innerOperation()

	switch op.(type) {
	case *OpRenameColumn:
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

	case o.Check != "":
		return &OpSetCheckConstraint{
			Table:  o.Table,
			Column: o.Column,
			Check:  o.Check,
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
	}
	return nil
}

// oneChange ensures that the 'alter column' operation attempts to make
// only one kind of change. For example, it should not attempt to rename a
// column and change its type at the same time.
func (o *OpAlterColumn) oneChange() bool {
	fieldsSet := 0

	if o.Name != "" {
		fieldsSet++
	}
	if o.Type != "" {
		fieldsSet++
	}
	if o.Check != "" {
		fieldsSet++
	}
	if o.References != nil {
		fieldsSet++
	}
	if o.NotNull != nil {
		fieldsSet++
	}

	return fieldsSet == 1
}
