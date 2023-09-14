package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pg-roll/pkg/schema"
)

type OpAddColumn struct {
	Table  string  `json:"table"`
	Up     *string `json:"up"`
	Column Column  `json:"column"`
}

var _ Operation = (*OpAddColumn)(nil)

func (o *OpAddColumn) Start(ctx context.Context, conn *sql.DB, stateSchema string, s *schema.Schema) error {
	table := s.GetTable(o.Table)

	if err := addColumn(ctx, conn, *o, table); err != nil {
		return fmt.Errorf("failed to start add column operation: %w", err)
	}

	if !o.Column.Nullable && o.Column.Default == nil {
		if err := addNotNullConstraint(ctx, conn, o.Table, o.Column.Name, TemporaryName(o.Column.Name)); err != nil {
			return fmt.Errorf("failed to add check constraint: %w", err)
		}
	}

	if o.Up != nil {
		err := createTrigger(ctx, conn, triggerConfig{
			Name:           TriggerName(o.Table, o.Column.Name),
			Direction:      TriggerDirectionUp,
			Columns:        s.GetTable(o.Table).Columns,
			SchemaName:     s.Name,
			TableName:      o.Table,
			PhysicalColumn: TemporaryName(o.Column.Name),
			StateSchema:    stateSchema,
			SQL:            *o.Up,
		})
		if err != nil {
			return fmt.Errorf("failed to create trigger: %w", err)
		}
		if err := backFill(ctx, conn, o.Table, TemporaryName(o.Column.Name)); err != nil {
			return fmt.Errorf("failed to backfill column: %w", err)
		}
	}

	table.AddColumn(o.Column.Name, schema.Column{
		Name: TemporaryName(o.Column.Name),
	})

	return nil
}

func (o *OpAddColumn) Complete(ctx context.Context, conn *sql.DB) error {
	tempName := TemporaryName(o.Column.Name)

	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME COLUMN %s TO %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(tempName),
		pq.QuoteIdentifier(o.Column.Name),
	))
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		pq.QuoteIdentifier(TriggerFunctionName(o.Table, o.Column.Name))))
	if err != nil {
		return err
	}

	if !o.Column.Nullable && o.Column.Default == nil {
		_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s VALIDATE CONSTRAINT %s",
			pq.QuoteIdentifier(o.Table),
			pq.QuoteIdentifier(NotNullConstraintName(o.Column.Name))))
		if err != nil {
			return err
		}

		_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s ALTER COLUMN %s SET NOT NULL",
			pq.QuoteIdentifier(o.Table),
			pq.QuoteIdentifier(o.Column.Name)))
		if err != nil {
			return err
		}

		_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s DROP CONSTRAINT IF EXISTS %s",
			pq.QuoteIdentifier(o.Table),
			pq.QuoteIdentifier(NotNullConstraintName(o.Column.Name))))
		if err != nil {
			return err
		}
	}

	return err
}

func (o *OpAddColumn) Rollback(ctx context.Context, conn *sql.DB) error {
	tempName := TemporaryName(o.Column.Name)

	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s DROP COLUMN IF EXISTS %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(tempName)))
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		pq.QuoteIdentifier(TriggerFunctionName(o.Table, o.Column.Name))))
	return err
}

func (o *OpAddColumn) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	if table.GetColumn(o.Column.Name) != nil {
		return ColumnAlreadyExistsError{Name: o.Column.Name, Table: o.Table}
	}

	if !o.Column.Nullable && o.Column.Default == nil && o.Up == nil {
		return FieldRequiredError{Name: "up"}
	}

	if o.Column.PrimaryKey {
		return errors.New("adding primary key columns is not supported")
	}

	return nil
}

func addColumn(ctx context.Context, conn *sql.DB, o OpAddColumn, t *schema.Table) error {
	// don't add non-nullable columns with no default directly
	// they are handled by:
	// - adding the column as nullable
	// - adding a NOT VALID check constraint on the column
	// - validating the constraint and converting the column to not null
	//   on migration completion
	// This is to avoid unnecessary exclusive table locks.
	if !o.Column.Nullable && o.Column.Default == nil {
		o.Column.Nullable = true
	}

	o.Column.Name = TemporaryName(o.Column.Name)

	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s",
		pq.QuoteIdentifier(t.Name),
		ColumnToSQL(o.Column),
	))
	return err
}

func addNotNullConstraint(ctx context.Context, conn *sql.DB, table, column, physicalColumn string) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s IS NOT NULL) NOT VALID",
		pq.QuoteIdentifier(table),
		pq.QuoteIdentifier(NotNullConstraintName(column)),
		pq.QuoteIdentifier(physicalColumn),
	))
	return err
}

func backFill(ctx context.Context, conn *sql.DB, table, column string) error {
	// touch rows without changing them in order to have the trigger fire
	// and set the value using the `up` SQL.
	// TODO: this should be done in batches in case of large tables.
	_, err := conn.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET %s = %s",
		pq.QuoteIdentifier(table),
		pq.QuoteIdentifier(column),
		pq.QuoteIdentifier(column)))

	return err
}

func NotNullConstraintName(columnName string) string {
	return "_pgroll_add_column_check_" + columnName
}
