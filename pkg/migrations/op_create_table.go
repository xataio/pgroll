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

var _ Operation = (*OpCreateTable)(nil)

func (o *OpCreateTable) Start(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	// Generate SQL for the columns in the table
	columnsSQL, err := columnsToSQL(o.Columns, tr)
	if err != nil {
		return nil, fmt.Errorf("failed to create columns SQL: %w", err)
	}

	// Create the table under a temporary name
	tempName := TemporaryName(o.Name)
	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (%s)",
		pq.QuoteIdentifier(tempName),
		columnsSQL))
	if err != nil {
		return nil, err
	}

	// Add comments to any columns that have them
	for _, col := range o.Columns {
		if col.Comment != nil {
			if err := addCommentToColumn(ctx, conn, tempName, col.Name, col.Comment); err != nil {
				return nil, fmt.Errorf("failed to add comment to column: %w", err)
			}
		}
	}

	// Add comment to the table itself
	if o.Comment != nil {
		if err := addCommentToTable(ctx, conn, tempName, o.Comment); err != nil {
			return nil, fmt.Errorf("failed to add comment to table: %w", err)
		}
	}

	columns := make(map[string]schema.Column, len(o.Columns))
	for _, col := range o.Columns {
		columns[col.Name] = schema.Column{
			Name: col.Name,
		}
	}

	s.AddTable(o.Name, schema.Table{
		Name:    tempName,
		Columns: columns,
	})

	return nil, nil
}

func (o *OpCreateTable) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	tempName := TemporaryName(o.Name)
	fmt.Println("create table complete")
	s.RemoveTemporaryResourceName(tempName)
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME TO %s",
		pq.QuoteIdentifier(tempName),
		pq.QuoteIdentifier(o.Name)))
	return err
}

func (o *OpCreateTable) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer) error {
	tempName := TemporaryName(o.Name)

	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		pq.QuoteIdentifier(tempName)))
	return err
}

func (o *OpCreateTable) Validate(ctx context.Context, s *schema.Schema) error {
	if err := ValidateIdentifierLength(o.Name); err != nil {
		return err
	}

	table := s.GetTable(o.Name)
	if table != nil {
		return TableAlreadyExistsError{Name: o.Name}
	}

	for _, col := range o.Columns {
		if err := ValidateIdentifierLength(col.Name); err != nil {
			return fmt.Errorf("invalid column: %w", err)
		}

		// Ensure that any foreign key references are valid, ie. the referenced
		// table and column exist.
		if col.References != nil {
			if err := col.References.Validate(s); err != nil {
				return ColumnReferenceError{
					Table:  o.Name,
					Column: col.Name,
					Err:    err,
				}
			}
		}

		// Ensure that any CHECK constraints are valid.
		if col.Check != nil {
			if err := col.Check.Validate(); err != nil {
				return CheckConstraintError{
					Table:  o.Name,
					Column: col.Name,
					Err:    err,
				}
			}
		}
	}
	s.AddTemporaryResourceName(TemporaryName(o.Name), o.Name)

	return nil
}

func (o *OpCreateTable) DeriveSchema(ctx context.Context, s *schema.Schema) error {
	s.AddTable(o.Name, schema.Table{
		Name:    o.Name,
		Columns: columns,
	})
	return nil
}

func columnsToSQL(cols []Column, tr SQLTransformer) (string, error) {
	var sql string
	var primaryKeys []string
	columnWriter := ColumnSQLWriter{WithPK: false, Transformer: tr}
	for i, col := range cols {
		if i > 0 {
			sql += ", "
		}
		colSQL, err := columnWriter.Write(col)
		if err != nil {
			return "", err
		}
		sql += colSQL

		if col.IsPrimaryKey() {
			primaryKeys = append(primaryKeys, pq.QuoteIdentifier(col.Name))
		}
	}

	if len(primaryKeys) > 0 {
		sql += fmt.Sprintf(", PRIMARY KEY (%s)", strings.Join(primaryKeys, ", "))
	}
	return sql, nil
}
