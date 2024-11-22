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
	columns := make([]*schema.Column, len(o.Columns))
	for i, colName := range o.Columns {
		columns[i] = table.GetColumn(colName)
	}

	d := NewColumnDuplicator(conn, table, columns...)
	if err := d.Duplicate(ctx); err != nil {
		return nil, fmt.Errorf("failed to duplicate columns for new constraint: %w", err)
	}

	// Setup triggers
	for _, colName := range o.Columns {
		upSQL := o.Up[colName]
		physicalColumnName := TemporaryName(colName)
		err := createTrigger(ctx, conn, tr, triggerConfig{
			Name:           TriggerName(o.Table, colName),
			Direction:      TriggerDirectionUp,
			Columns:        table.Columns,
			SchemaName:     s.Name,
			LatestSchema:   latestSchema,
			TableName:      o.Table,
			PhysicalColumn: physicalColumnName,
			SQL:            upSQL,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create up trigger: %w", err)
		}

		table.AddColumn(colName, schema.Column{
			Name: physicalColumnName,
		})

		downSQL := o.Down[colName]
		err = createTrigger(ctx, conn, tr, triggerConfig{
			Name:           TriggerName(o.Table, physicalColumnName),
			Direction:      TriggerDirectionDown,
			Columns:        table.Columns,
			LatestSchema:   latestSchema,
			SchemaName:     s.Name,
			TableName:      o.Table,
			PhysicalColumn: colName,
			SQL:            downSQL,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create down trigger: %w", err)
		}
	}

	switch o.Type {
	case OpCreateConstraintTypeUnique:
		return table, o.addUniqueIndex(ctx, conn)
	case OpCreateConstraintTypeCheck:
		return table, o.addCheckConstraint(ctx, conn)
	case OpCreateConstraintTypeForeignKey:
		return table, o.addForeignKeyConstraint(ctx, conn)
	}

	return table, nil
}

func (o *OpCreateConstraint) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	switch o.Type {
	case OpCreateConstraintTypeUnique:
		uniqueOp := &OpSetUnique{
			Table: o.Table,
			Name:  o.Name,
		}
		err := uniqueOp.Complete(ctx, conn, tr, s)
		if err != nil {
			return err
		}
	case OpCreateConstraintTypeCheck:
		checkOp := &OpSetCheckConstraint{
			Table: o.Table,
			Check: CheckConstraint{
				Name: o.Name,
			},
		}
		err := checkOp.Complete(ctx, conn, tr, s)
		if err != nil {
			return err
		}
	case OpCreateConstraintTypeForeignKey:
		fkOp := &OpSetForeignKey{
			Table: o.Table,
			References: ForeignKeyReference{
				Name: o.Name,
			},
		}
		err := fkOp.Complete(ctx, conn, tr, s)
		if err != nil {
			return err
		}
	}

	// remove old columns
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s %s",
		pq.QuoteIdentifier(o.Table),
		dropMultipleColumns(quoteColumnNames(o.Columns)),
	))
	if err != nil {
		return err
	}

	// rename new columns to old name
	table := s.GetTable(o.Table)
	for _, col := range o.Columns {
		column := table.GetColumn(col)
		if err := RenameDuplicatedColumn(ctx, conn, table, column); err != nil {
			return err
		}
	}

	return o.removeTriggers(ctx, conn)
}

func (o *OpCreateConstraint) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s %s",
		pq.QuoteIdentifier(o.Table),
		dropMultipleColumns(quotedTemporaryNames(o.Columns)),
	))
	if err != nil {
		return err
	}

	return o.removeTriggers(ctx, conn)
}

func (o *OpCreateConstraint) removeTriggers(ctx context.Context, conn db.DB) error {
	dropFuncs := make([]string, len(o.Columns)*2)
	for i, j := 0, 0; i < len(o.Columns); i, j = i+1, j+2 {
		dropFuncs[j] = pq.QuoteIdentifier(TriggerFunctionName(o.Table, o.Columns[i]))
		dropFuncs[j+1] = pq.QuoteIdentifier(TriggerFunctionName(o.Table, TemporaryName(o.Columns[i])))
	}
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		strings.Join(dropFuncs, ", "),
	))
	return err
}

func dropMultipleColumns(columns []string) string {
	for i, col := range columns {
		columns[i] = "DROP COLUMN IF EXISTS " + col
	}
	return strings.Join(columns, ", ")
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
		if _, ok := o.Up[col]; !ok {
			return ColumnMigrationMissingError{
				Table: o.Table,
				Name:  col,
			}
		}
		if _, ok := o.Down[col]; !ok {
			return ColumnMigrationMissingError{
				Table: o.Table,
				Name:  col,
			}
		}
	}

	switch o.Type {
	case OpCreateConstraintTypeUnique:
		if len(o.Columns) == 0 {
			return FieldRequiredError{Name: "columns"}
		}
	case OpCreateConstraintTypeCheck:
		if o.Check == nil || *o.Check == "" {
			return FieldRequiredError{Name: "check"}
		}
	case OpCreateConstraintTypeForeignKey:
		if o.References == nil {
			return FieldRequiredError{Name: "references"}
		}
		table := s.GetTable(o.References.Table)
		if table == nil {
			return TableDoesNotExistError{Name: o.References.Table}
		}
		for _, col := range o.References.Columns {
			if table.GetColumn(col) == nil {
				return ColumnDoesNotExistError{
					Table: o.References.Table,
					Name:  col,
				}
			}
		}
	}

	return nil
}

func (o *OpCreateConstraint) addUniqueIndex(ctx context.Context, conn db.DB) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)",
		pq.QuoteIdentifier(o.Name),
		pq.QuoteIdentifier(o.Table),
		strings.Join(quotedTemporaryNames(o.Columns), ", "),
	))

	return err
}

func (o *OpCreateConstraint) addCheckConstraint(ctx context.Context, conn db.DB) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s) NOT VALID",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.Name),
		rewriteCheckExpression(*o.Check, o.Columns...),
	))

	return err
}

func (o *OpCreateConstraint) addForeignKeyConstraint(ctx context.Context, conn db.DB) error {
	onDelete := "NO ACTION"
	if o.References.OnDelete != "" {
		onDelete = strings.ToUpper(string(o.References.OnDelete))
	}

	_, err := conn.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON DELETE %s NOT VALID",
			pq.QuoteIdentifier(o.Table),
			pq.QuoteIdentifier(o.Name),
			strings.Join(quotedTemporaryNames(o.Columns), ","),
			pq.QuoteIdentifier(o.References.Table),
			strings.Join(quoteColumnNames(o.References.Columns), ","),
			onDelete,
		))

	return err
}

func quotedTemporaryNames(columns []string) []string {
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = pq.QuoteIdentifier(TemporaryName(col))
	}
	return names
}
