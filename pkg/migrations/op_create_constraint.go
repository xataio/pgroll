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
	var err error
	var table *schema.Table
	for _, col := range o.Columns {
		if table, err = o.duplicateColumnBeforeStart(ctx, conn, latestSchema, tr, col, s); err != nil {
			return nil, err
		}
	}

	switch o.Type { //nolint:gocritic // more cases will be added
	case OpCreateConstraintTypeUnique:
		return table, o.addUniqueIndex(ctx, conn)
	}

	return table, nil
}

func (o *OpCreateConstraint) duplicateColumnBeforeStart(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, colName string, s *schema.Schema) (*schema.Table, error) {
	table := s.GetTable(o.Table)
	column := table.GetColumn(colName)

	d := NewColumnDuplicator(conn, table, column)
	if err := d.Duplicate(ctx); err != nil {
		return nil, fmt.Errorf("failed to duplicate column for new constraint: %w", err)
	}

	upSQL, ok := o.Up[colName]
	if !ok {
		return nil, fmt.Errorf("up migration is missing for column %s", colName)
	}
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

	downSQL, ok := o.Down[colName]
	if !ok {
		return nil, fmt.Errorf("down migration is missing for column %s", colName)
	}
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
	return table, nil
}

func (o *OpCreateConstraint) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	switch o.Type { //nolint:gocritic // more cases will be added
	case OpCreateConstraintTypeUnique:
		uniqueOp := &OpSetUnique{
			Table: o.Table,
			Name:  o.Name,
		}
		err := uniqueOp.Complete(ctx, conn, tr, s)
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
		strings.Join(quotedTemporaryNames(o.Columns), ", "),
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
