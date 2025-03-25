// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/internal/defaults"
	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpAddColumn)(nil)

func (o *OpAddColumn) Start(ctx context.Context, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}

	// If the column has a DEFAULT, check if it can be added using the fast path
	// optimization
	fastPathDefault := false
	if o.Column.HasDefault() {
		v, err := defaults.UsesFastPath(ctx, conn, table.Name, o.Column.Type, *o.Column.Default)
		if err != nil {
			return nil, fmt.Errorf("failed to check for fast path default optimization: %w", err)
		}
		fastPathDefault = v
	}

	if err := addColumn(ctx, conn, *o, table, fastPathDefault); err != nil {
		return nil, fmt.Errorf("failed to start add column operation: %w", err)
	}

	if o.Column.Comment != nil {
		if err := addCommentToColumn(ctx, conn, table.Name, TemporaryName(o.Column.Name), o.Column.Comment); err != nil {
			return nil, fmt.Errorf("failed to add comment to column: %w", err)
		}
	}

	// If the column is `NOT NULL` and there is no default value (either because
	// the column as no DEFAULT or because the default value cannot be set using
	// the fast path optimization), add a NOT NULL constraint to the column which
	// will be validated on migration completion.
	if !o.Column.IsNullable() && (o.Column.Default == nil || !fastPathDefault) {
		if err := addNotNullConstraint(ctx, conn, table.Name, o.Column.Name, TemporaryName(o.Column.Name)); err != nil {
			return nil, fmt.Errorf("failed to add not null constraint: %w", err)
		}
	}

	if o.Column.Check != nil {
		if err := o.addCheckConstraint(ctx, table.Name, conn); err != nil {
			return nil, fmt.Errorf("failed to add check constraint: %w", err)
		}
	}

	if o.Column.Unique {
		if err := createUniqueIndexConcurrently(ctx, conn, s.Name, UniqueIndexName(o.Column.Name), table.Name, []string{TemporaryName(o.Column.Name)}); err != nil {
			return nil, fmt.Errorf("failed to add unique index: %w", err)
		}
	}

	// If the column has a DEFAULT that cannot be set using the fast path
	// optimization, the `up` SQL expression must be used to set the DEFAULT
	// value for the column.
	if o.Column.HasDefault() && !fastPathDefault {
		if o.Up != *o.Column.Default {
			return nil, UpSQLMustBeColumnDefaultError{Column: o.Column.Name}
		}
	}

	var tableToBackfill *schema.Table
	if o.Up != "" {
		err := NewCreateTriggerAction(conn,
			triggerConfig{
				Name:           TriggerName(o.Table, o.Column.Name),
				Direction:      TriggerDirectionUp,
				Columns:        table.Columns,
				SchemaName:     s.Name,
				LatestSchema:   latestSchema,
				TableName:      table.Name,
				PhysicalColumn: TemporaryName(o.Column.Name),
				SQL:            o.Up,
			},
		).Execute(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create trigger: %w", err)
		}
		tableToBackfill = table
	}

	table.AddColumn(o.Column.Name, &schema.Column{
		Name: TemporaryName(o.Column.Name),
	})

	return tableToBackfill, nil
}

func (o *OpAddColumn) Complete(ctx context.Context, conn db.DB, s *schema.Schema) error {
	err := NewRenameColumnAction(conn, o.Table, TemporaryName(o.Column.Name), o.Column.Name).Execute(ctx)
	if err != nil {
		return err
	}

	err = NewDropFunctionAction(conn, TriggerFunctionName(o.Table, o.Column.Name)).Execute(ctx)
	if err != nil {
		return err
	}

	removeBackfillColumn := NewDropColumnAction(conn, o.Table, CNeedsBackfillColumn)
	err = removeBackfillColumn.Execute(ctx)
	if err != nil {
		return err
	}

	if !o.Column.IsNullable() && o.Column.Default == nil {
		err = upgradeNotNullConstraintToNotNullAttribute(ctx, conn, o.Table, o.Column.Name)
		if err != nil {
			return err
		}
	}

	if o.Column.Check != nil {
		_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s VALIDATE CONSTRAINT %s",
			pq.QuoteIdentifier(o.Table),
			pq.QuoteIdentifier(o.Column.Check.Name)))
		if err != nil {
			return err
		}
	}

	if o.Column.Unique {
		_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s_%s_key UNIQUE USING INDEX %s",
			pq.QuoteIdentifier(o.Table),
			o.Table,
			o.Column.Name,
			UniqueIndexName(o.Column.Name)))
		if err != nil {
			return err
		}
	}

	// If the column has a DEFAULT that could not be set using the fast-path
	// optimization, set it here.
	column := s.GetTable(o.Table).GetColumn(TemporaryName(o.Column.Name))
	if o.Column.HasDefault() && column.Default == nil {
		_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s ALTER COLUMN %s SET DEFAULT %s",
			pq.QuoteIdentifier(o.Table),
			pq.QuoteIdentifier(o.Column.Name),
			*o.Column.Default,
		))
		if err != nil {
			return err
		}

		// Validate the `NOT NULL` constraint on the column if necessary
		if !o.Column.IsNullable() {
			err = upgradeNotNullConstraintToNotNullAttribute(ctx, conn, o.Table, o.Column.Name)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (o *OpAddColumn) Rollback(ctx context.Context, conn db.DB, s *schema.Schema) error {
	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}
	column := table.GetColumn(o.Column.Name)
	if column == nil {
		return ColumnDoesNotExistError{Table: o.Table, Name: o.Column.Name}
	}

	rollbackAddColumn := NewDropColumnAction(conn, table.Name, column.Name)
	err := rollbackAddColumn.Execute(ctx)
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		pq.QuoteIdentifier(TriggerFunctionName(o.Table, o.Column.Name))))
	if err != nil {
		return err
	}

	removeBackfillColumn := NewDropColumnAction(conn, table.Name, CNeedsBackfillColumn)
	err = removeBackfillColumn.Execute(ctx)

	return err
}

func (o *OpAddColumn) Validate(ctx context.Context, s *schema.Schema) error {
	if err := ValidateIdentifierLength(o.Column.Name); err != nil {
		return err
	}

	// Validate that the column contains all required fields
	if !o.Column.Validate() {
		return ColumnIsInvalidError{Table: o.Table, Name: o.Column.Name}
	}

	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	if table.GetColumn(o.Column.Name) != nil {
		return ColumnAlreadyExistsError{Name: o.Column.Name, Table: o.Table}
	}

	if o.Column.References != nil {
		if err := o.Column.References.Validate(s); err != nil {
			return ColumnReferenceError{
				Table:  o.Table,
				Column: o.Column.Name,
				Err:    err,
			}
		}
	}

	if o.Column.Check != nil {
		if err := o.Column.Check.Validate(); err != nil {
			return CheckConstraintError{
				Table:  o.Table,
				Column: o.Column.Name,
				Err:    err,
			}
		}
	}

	// Ensure backfill is possible
	if o.Up != "" {
		err := backfill.IsPossible(table)
		if err != nil {
			return err
		}
	}

	if o.Column.Generated != nil && o.Column.Generated.Expression != "" && o.Column.Generated.Identity != nil {
		return InvalidGeneratedColumnError{Table: o.Table, Column: o.Column.Name}
	}

	if !o.Column.IsNullable() && o.Column.Default == nil && o.Up == "" && !o.Column.HasImplicitDefault() && o.Column.Generated == nil {
		return FieldRequiredError{Name: "up"}
	}

	if o.Column.IsPrimaryKey() {
		return errors.New("adding primary key columns is not supported")
	}

	// Update the schema to ensure that the new column is visible to validation of
	// subsequent operations.
	table.AddColumn(o.Column.Name, &schema.Column{
		Name: TemporaryName(o.Column.Name),
	})

	return nil
}

func addColumn(ctx context.Context, conn db.DB, o OpAddColumn, t *schema.Table, fastPathDefault bool) error {
	// don't add non-nullable columns with no default directly
	// they are handled by:
	// - adding the column as nullable
	// - adding a NOT VALID check constraint on the column
	// - validating the constraint and converting the column to not null
	//   on migration completion
	// This is to avoid unnecessary exclusive table locks.
	if !o.Column.IsNullable() && o.Column.Default == nil {
		o.Column.Nullable = true
	}

	if o.Column.Generated != nil {
		return fmt.Errorf("adding generated columns to existing tables is not supported")
	}

	// Don't add a column with a CHECK constraint directly.
	// They are handled by:
	// - adding the column without the constraint
	// - adding a NOT VALID check constraint to the column
	// - validating the constraint on migration completion
	// This is to avoid unnecessary exclusive table locks.
	o.Column.Check = nil

	// Don't add a column with a UNIQUE constraint directly.
	// They are handled by:
	// - adding the column without the UNIQUE modifier
	// - creating a UNIQUE index concurrently
	// - adding a UNIQUE constraint USING the index on migration completion
	// This is to avoid unnecessary exclusive table locks.
	o.Column.Unique = false

	// Don't add volatile DEFAULT values directly.
	// They are handled by:
	// - adding the column without a DEFAULT
	// - creating a trigger to backfill the column with the default
	// - adding the DEFAULT value on migration completion
	// This is to avoid unnecessary exclusive table locks.
	if !fastPathDefault {
		o.Column.Default = nil
		o.Column.Nullable = true
	}

	o.Column.Name = TemporaryName(o.Column.Name)
	columnWriter := ColumnSQLWriter{WithPK: true}
	colSQL, err := columnWriter.Write(o.Column)
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s",
		pq.QuoteIdentifier(t.Name),
		colSQL,
	))

	return err
}

// upgradeNotNullConstraintToNotNullAttribute validates and upgrades a NOT NULL
// constraint to a NOT NULL column attribute. The constraint is removed after
// the column attribute is added.
func upgradeNotNullConstraintToNotNullAttribute(ctx context.Context, conn db.DB, tableName, columnName string) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s VALIDATE CONSTRAINT %s",
		pq.QuoteIdentifier(tableName),
		pq.QuoteIdentifier(NotNullConstraintName(columnName))))
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s ALTER COLUMN %s SET NOT NULL",
		pq.QuoteIdentifier(tableName),
		pq.QuoteIdentifier(columnName)))
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s DROP CONSTRAINT IF EXISTS %s",
		pq.QuoteIdentifier(tableName),
		pq.QuoteIdentifier(NotNullConstraintName(columnName))))

	return err
}

func addNotNullConstraint(ctx context.Context, conn db.DB, table, column, physicalColumn string) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s IS NOT NULL) NOT VALID",
		pq.QuoteIdentifier(table),
		pq.QuoteIdentifier(NotNullConstraintName(column)),
		pq.QuoteIdentifier(physicalColumn),
	))
	return err
}

func (o *OpAddColumn) addCheckConstraint(ctx context.Context, tableName string, conn db.DB) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s) NOT VALID",
		pq.QuoteIdentifier(tableName),
		pq.QuoteIdentifier(o.Column.Check.Name),
		rewriteCheckExpression(o.Column.Check.Constraint, o.Column.Name),
	))
	return err
}

// UniqueIndexName returns the name of the unique index for the given column
func UniqueIndexName(columnName string) string {
	return "_pgroll_uniq_" + columnName
}

// NotNullConstraintName returns the name of the NOT NULL constraint for the given column
func NotNullConstraintName(columnName string) string {
	return "_pgroll_check_not_null_" + columnName
}

// IsNotNullConstraintName returns true if the given name is a NOT NULL constraint name
func IsNotNullConstraintName(name string) bool {
	return strings.HasPrefix(name, "_pgroll_check_not_null_")
}

// ColumnSQLWriter writes a column to SQL
// It can optionally include the primary key constraint
// When creating a table, the primary key constraint is not added to the column definition
type ColumnSQLWriter struct {
	WithPK bool
}

func (w ColumnSQLWriter) Write(col Column) (string, error) {
	sql := fmt.Sprintf("%s %s", pq.QuoteIdentifier(col.Name), col.Type)

	if w.WithPK && col.IsPrimaryKey() {
		sql += " PRIMARY KEY"
	}

	if col.IsUnique() {
		sql += " UNIQUE"
	}
	if !col.IsNullable() {
		sql += " NOT NULL"
	}
	if col.Default != nil {
		sql += fmt.Sprintf(" DEFAULT %s", *col.Default)
	}

	if col.Generated != nil {
		if col.Generated.Expression != "" {
			sql += fmt.Sprintf(" GENERATED ALWAYS AS (%s) STORED", col.Generated.Expression)
		} else if col.Generated.Identity != nil {
			sql += fmt.Sprintf(" GENERATED %s AS IDENTITY", col.Generated.Identity.UserSpecifiedValues)
			if col.Generated.Identity.SequenceOptions != "" {
				sql += fmt.Sprintf(" (%s)", col.Generated.Identity.SequenceOptions)
			}
		}
	}

	if col.References != nil {
		writer := &ConstraintSQLWriter{Name: col.References.Name}
		sql += " " + writer.WriteForeignKey(
			col.References.Table,
			[]string{col.References.Column},
			col.References.OnDelete,
			col.References.OnUpdate,
			nil,
			col.References.MatchType)
	}
	if col.Check != nil {
		writer := &ConstraintSQLWriter{Name: col.Check.Name}
		sql += " " + writer.WriteCheck(col.Check.Constraint, col.Check.NoInherit)
	}
	return sql, nil
}
