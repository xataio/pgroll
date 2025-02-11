// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpAddColumn)(nil)

func (o *OpAddColumn) Start(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, s *schema.Schema) (*schema.Table, error) {
	table := s.GetTable(o.Table)

	if err := addColumn(ctx, conn, *o, table, tr); err != nil {
		return nil, fmt.Errorf("failed to start add column operation: %w", err)
	}

	if o.Column.Comment != nil {
		if err := addCommentToColumn(ctx, conn, table.Name, TemporaryName(o.Column.Name), o.Column.Comment); err != nil {
			return nil, fmt.Errorf("failed to add comment to column: %w", err)
		}
	}

	if !o.Column.IsNullable() && o.Column.Default == nil {
		if err := addNotNullConstraint(ctx, conn, table.Name, o.Column.Name, TemporaryName(o.Column.Name)); err != nil {
			return nil, fmt.Errorf("failed to add not null constraint: %w", err)
		}
	}

	if o.Column.Check != nil {
		if err := o.addCheckConstraint(ctx, table.Name, conn); err != nil {
			return nil, fmt.Errorf("failed to add check constraint: %w", err)
		}
	}

	var tableToBackfill *schema.Table
	if o.Up != "" {
		err := createTrigger(ctx, conn, tr, triggerConfig{
			Name:           TriggerName(o.Table, o.Column.Name),
			Direction:      TriggerDirectionUp,
			Columns:        table.Columns,
			SchemaName:     s.Name,
			LatestSchema:   latestSchema,
			TableName:      table.Name,
			PhysicalColumn: TemporaryName(o.Column.Name),
			SQL:            o.Up,
		})
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

func (o *OpAddColumn) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
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

	if !o.Column.IsNullable() && o.Column.Default == nil {
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

	if o.Column.Check != nil {
		_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s VALIDATE CONSTRAINT %s",
			pq.QuoteIdentifier(o.Table),
			pq.QuoteIdentifier(o.Column.Check.Name)))
		if err != nil {
			return err
		}
	}

	// Add the column to the in-memory schema so that Complete steps in subsequent
	// operations can see the new column.
	table := s.GetTable(o.Table)
	table.AddColumn(o.Column.Name, &schema.Column{
		Name: o.Column.Name,
	})

	return err
}

func (o *OpAddColumn) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	table := s.GetTable(o.Table)
	column := table.GetColumn(o.Column.Name)

	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s DROP COLUMN IF EXISTS %s",
		pq.QuoteIdentifier(table.Name),
		pq.QuoteIdentifier(column.Name)))
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		pq.QuoteIdentifier(TriggerFunctionName(o.Table, o.Column.Name))))
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

func addColumn(ctx context.Context, conn db.DB, o OpAddColumn, t *schema.Table, tr SQLTransformer) error {
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

	o.Column.Name = TemporaryName(o.Column.Name)
	columnWriter := ColumnSQLWriter{WithPK: true, Transformer: tr}
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
	WithPK      bool
	Transformer SQLTransformer
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
		d, err := w.Transformer.TransformSQL(*col.Default)
		if err != nil {
			return "", err
		}
		sql += fmt.Sprintf(" DEFAULT %s", d)
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
		sql += fmt.Sprintf(" CONSTRAINT %s CHECK (%s)",
			pq.QuoteIdentifier(col.Check.Name),
			col.Check.Constraint)
	}
	return sql, nil
}
