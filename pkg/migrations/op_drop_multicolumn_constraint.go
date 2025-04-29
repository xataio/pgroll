// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"
	"slices"

	"github.com/lib/pq"
	"github.com/pterm/pterm"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpDropMultiColumnConstraint)(nil)

func (o *OpDropMultiColumnConstraint) Start(ctx context.Context, logger pterm.Logger, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	logger.Info("starting operation", logger.Args(o.loggerArgs()...))

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}

	// Get all columns covered by the constraint to be dropped
	constraintColumns := table.GetConstraintColumns(o.Name)
	columns := make([]*schema.Column, len(constraintColumns))
	for i, c := range constraintColumns {
		columns[i] = table.GetColumn(c)
		if columns[i] == nil {
			return nil, ColumnDoesNotExistError{Table: o.Table, Name: c}
		}
	}

	// Duplicate each of the columns covered by the constraint to be dropped.
	// Each column is duplicated assuming its final name after the migration is
	// completed.
	d := NewColumnDuplicator(conn, table, columns...).WithoutConstraint(o.Name)
	for _, colName := range constraintColumns {
		d = d.WithName(table.GetColumn(colName).Name, TemporaryName(colName))
	}
	if err := d.Duplicate(ctx); err != nil {
		return nil, fmt.Errorf("failed to duplicate column: %w", err)
	}

	// Create triggers for each column covered by the constraint to be dropped
	for _, columnName := range table.GetConstraintColumns(o.Name) {
		// Add a trigger to copy values from the old column to the new, rewriting values using the `up` SQL.
		err := NewCreateTriggerAction(conn,
			triggerConfig{
				Name:           TriggerName(o.Table, columnName),
				Direction:      TriggerDirectionUp,
				Columns:        table.Columns,
				SchemaName:     s.Name,
				LatestSchema:   latestSchema,
				TableName:      table.Name,
				PhysicalColumn: TemporaryName(columnName),
				SQL:            o.upSQL(columnName),
			},
		).Execute(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create up trigger: %w", err)
		}

		// Add the new column to the internal schema representation. This is done
		// here, before creation of the down trigger, so that the trigger can declare
		// a variable for the new column. Save the old column name for use as the
		// physical column name in the down trigger first.
		oldPhysicalColumn := table.GetColumn(columnName).Name
		table.AddColumn(columnName, &schema.Column{
			Name: TemporaryName(columnName),
		})

		// Add a trigger to copy values from the new column to the old, rewriting values using the `down` SQL.
		err = NewCreateTriggerAction(conn,
			triggerConfig{
				Name:           TriggerName(o.Table, TemporaryName(columnName)),
				Direction:      TriggerDirectionDown,
				Columns:        table.Columns,
				SchemaName:     s.Name,
				LatestSchema:   latestSchema,
				TableName:      table.Name,
				PhysicalColumn: oldPhysicalColumn,
				SQL:            o.Down[columnName],
			},
		).Execute(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create down trigger: %w", err)
		}
	}

	return table, nil
}

func (o *OpDropMultiColumnConstraint) Complete(ctx context.Context, logger pterm.Logger, conn db.DB, s *schema.Schema) error {
	logger.Info("completing operation", logger.Args(o.loggerArgs()...))

	table := s.GetTable(o.Table)

	for _, columnName := range table.GetConstraintColumns(o.Name) {
		// Remove the up and down function and trigger
		err := NewDropFunctionAction(conn, TriggerFunctionName(o.Table, columnName), TriggerFunctionName(o.Table, TemporaryName(columnName))).Execute(ctx)
		if err != nil {
			return err
		}

		if err := alterSequenceOwnerToDuplicatedColumn(ctx, conn, o.Table, columnName); err != nil {
			return err
		}

		removeBackfillColumn := NewDropColumnAction(conn, o.Table, backfill.CNeedsBackfillColumn)
		err = removeBackfillColumn.Execute(ctx)
		if err != nil {
			return err
		}

		removeOldColumn := NewDropColumnAction(conn, o.Table, columnName)
		err = removeOldColumn.Execute(ctx)
		if err != nil {
			return err
		}

		// Rename the new column to the old column name
		column := table.GetColumn(columnName)
		if err := RenameDuplicatedColumn(ctx, conn, table, column); err != nil {
			return err
		}
	}

	return nil
}

func (o *OpDropMultiColumnConstraint) Rollback(ctx context.Context, logger pterm.Logger, conn db.DB, s *schema.Schema) error {
	logger.Info("rolling back operation", logger.Args(o.loggerArgs()...))

	table := s.GetTable(o.Table)

	for _, columnName := range table.GetConstraintColumns(o.Name) {
		removeNewColumn := NewDropColumnAction(conn, table.Name, TemporaryName(columnName))
		err := removeNewColumn.Execute(ctx)
		if err != nil {
			return err
		}

		// Remove the up and down function and trigger
		err = NewDropFunctionAction(conn, TriggerFunctionName(o.Table, columnName), TriggerFunctionName(o.Table, TemporaryName(columnName))).Execute(ctx)
		if err != nil {
			return err
		}

		removeBackfillColumn := NewDropColumnAction(conn, table.Name, backfill.CNeedsBackfillColumn)
		err = removeBackfillColumn.Execute(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *OpDropMultiColumnConstraint) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	if o.Name == "" {
		return FieldRequiredError{Name: "name"}
	}

	if !table.ConstraintExists(o.Name) {
		return ConstraintDoesNotExistError{Table: o.Table, Constraint: o.Name}
	}

	if o.Down == nil {
		return FieldRequiredError{Name: "down"}
	}

	// Ensure that `down` migrations are present for all columns covered by the
	// constraint to be dropped.
	for _, columnName := range table.GetConstraintColumns(o.Name) {
		if _, ok := o.Down[columnName]; !ok {
			return ColumnMigrationMissingError{
				Table: o.Table,
				Name:  columnName,
			}
		}
	}

	// Ensure that only columns covered by the constraint are present in the
	// `up` and `down` migrations.
	for _, m := range []map[string]string{o.Down, o.Up} {
		for columnName := range m {
			if !slices.Contains(table.GetConstraintColumns(o.Name), columnName) {
				return ColumnMigrationRedundantError{
					Table: o.Table,
					Name:  columnName,
				}
			}
		}
	}

	return nil
}

func (o *OpDropMultiColumnConstraint) upSQL(column string) string {
	if o.Up[column] != "" {
		return o.Up[column]
	}

	return pq.QuoteIdentifier(column)
}

func (o *OpDropMultiColumnConstraint) loggerArgs() []any {
	return []any{
		"operation", OpNameDropMultiColumnConstraint,
		"constraint", o.Name,
		"table", o.Table,
	}
}
