// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpAddColumn)(nil)

func (o *OpAddColumn) Start(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	table := s.GetTable(o.Table)

	if err := addColumn(ctx, conn, *o, table, tr); err != nil {
		return nil, fmt.Errorf("failed to start add column operation: %w", err)
	}

	if o.Column.Comment != nil {
		if err := addCommentToColumn(ctx, conn, o.Table, TemporaryName(o.Column.Name), o.Column.Comment); err != nil {
			return nil, fmt.Errorf("failed to add comment to column: %w", err)
		}
	}

	if !o.Column.IsNullable() && o.Column.Default == nil {
		if err := addNotNullConstraint(ctx, conn, o.Table, o.Column.Name, TemporaryName(o.Column.Name)); err != nil {
			return nil, fmt.Errorf("failed to add not null constraint: %w", err)
		}
	}

	if o.Column.Check != nil {
		if err := o.addCheckConstraint(ctx, conn); err != nil {
			return nil, fmt.Errorf("failed to add check constraint: %w", err)
		}
	}

	var tableToBackfill *schema.Table
	if o.Up != "" {
		err := createTrigger(ctx, conn, tr, triggerConfig{
			Name:           TriggerName(o.Table, o.Column.Name),
			Direction:      TriggerDirectionUp,
			Columns:        s.GetTable(o.Table).Columns,
			SchemaName:     s.Name,
			LatestSchema:   latestSchema,
			TableName:      o.Table,
			PhysicalColumn: TemporaryName(o.Column.Name),
			SQL:            o.Up,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create trigger: %w", err)
		}
		tableToBackfill = table
	}

	table.AddColumn(o.Column.Name, schema.Column{
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

	return err
}

func (o *OpAddColumn) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer) error {
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
		err := checkBackfill(table)
		if err != nil {
			return err
		}
	}

	if !o.Column.IsNullable() && o.Column.Default == nil && o.Up == "" {
		return FieldRequiredError{Name: "up"}
	}

	if o.Column.IsPrimaryKey() {
		return errors.New("adding primary key columns is not supported")
	}

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
		o.Column.Nullable = ptr(true)
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
	fmt.Println(colSQL)
	if err != nil {
		return err
	}

	fmt.Printf("ALTER TABLE %s ADD COLUMN %s\n", pq.QuoteIdentifier(t.Name), colSQL)

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

func (o *OpAddColumn) addCheckConstraint(ctx context.Context, conn db.DB) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s) NOT VALID",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.Column.Check.Name),
		rewriteCheckExpression(o.Column.Check.Constraint, o.Column.Name, TemporaryName(o.Column.Name)),
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
type ColumnSQLWriter struct {
	WithPK           bool
	WithFKConstraint bool
	Transformer      SQLTransformer
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
	if col.References != nil {
		onDelete := "NO ACTION"
		if col.References.OnDelete != "" {
			onDelete = strings.ToUpper(string(col.References.OnDelete))
		}
		var fkName, references string
		if col.References.Column != nil {
			fkName = pq.QuoteIdentifier(col.References.Name)
			references = fmt.Sprintf("%s(%s)", pq.QuoteIdentifier(col.References.Table), pq.QuoteIdentifier(*col.References.Column))
		}
		if col.References.Columns != nil {
			quotedCols := make([]string, len(col.References.Columns))
			for i, c := range col.References.Columns {
				quotedCols[i] = pq.QuoteIdentifier(c)
			}
			fkName = "FOREIGN KEY " + pq.QuoteIdentifier(col.References.Name)
			references = fmt.Sprintf("%s(%s)", pq.QuoteIdentifier(col.References.Table), strings.Join(quotedCols, ", "))
		}

		sql += fmt.Sprintf(" CONSTRAINT %s REFERENCES %s ON DELETE %s",
			fkName,
			references,
			onDelete)
	}
	if col.Check != nil {
		sql += fmt.Sprintf(" CONSTRAINT %s CHECK (%s)",
			pq.QuoteIdentifier(col.Check.Name),
			col.Check.Constraint)
	}
	fmt.Println(sql)
	return sql, nil
}
