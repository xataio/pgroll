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

	constraintsSQL, err := constraintsToSQL(o.Constraints)
	if err != nil {
		return nil, fmt.Errorf("failed to create constraints SQL: %w", err)
	}

	// Create the table
	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (%s %s)",
		pq.QuoteIdentifier(o.Name),
		columnsSQL,
		constraintsSQL))
	if err != nil {
		return nil, err
	}

	// Add comments to any columns that have them
	for _, col := range o.Columns {
		if col.Comment != nil {
			if err := addCommentToColumn(ctx, conn, o.Name, col.Name, col.Comment); err != nil {
				return nil, fmt.Errorf("failed to add comment to column: %w", err)
			}
		}
	}

	// Add comment to the table itself
	if o.Comment != nil {
		if err := addCommentToTable(ctx, conn, o.Name, o.Comment); err != nil {
			return nil, fmt.Errorf("failed to add comment to table: %w", err)
		}
	}

	// Update the in-memory schema representation with the new table
	o.updateSchema(s)

	return nil, nil
}

func (o *OpCreateTable) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	// No-op
	return nil
}

func (o *OpCreateTable) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		pq.QuoteIdentifier(o.Name)))
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

	for _, c := range o.Constraints {
		if c.Name == "" {
			return FieldRequiredError{Name: "name"}
		}
		if err := ValidateIdentifierLength(c.Name); err != nil {
			return fmt.Errorf("invalid constraint: %w", err)
		}

		switch c.Type { //nolint:gocritic // more cases are coming soon
		case ConstraintTypeUnique:
			if len(c.Columns) == 0 {
				return FieldRequiredError{Name: "columns"}
			}
		}
	}

	// Update the schema to ensure that the new table is visible to validation of
	// subsequent operations.
	o.updateSchema(s)

	return nil
}

// updateSchema updates the in-memory schema representation with the details of
// the new table.
func (o *OpCreateTable) updateSchema(s *schema.Schema) *schema.Schema {
	columns := make(map[string]*schema.Column, len(o.Columns))
	for _, col := range o.Columns {
		columns[col.Name] = &schema.Column{
			Name: col.Name,
		}
	}
	var uniqueConstraints map[string]*schema.UniqueConstraint
	for _, c := range o.Constraints {
		switch c.Type { //nolint:gocritic // more cases are coming soon
		case ConstraintTypeUnique:
			if uniqueConstraints == nil {
				uniqueConstraints = make(map[string]*schema.UniqueConstraint)
			}
			uniqueConstraints[c.Name] = &schema.UniqueConstraint{
				Name:    c.Name,
				Columns: c.Columns,
			}
		}
	}
	s.AddTable(o.Name, &schema.Table{
		Name:              o.Name,
		Columns:           columns,
		UniqueConstraints: uniqueConstraints,
	})

	return s
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

func constraintsToSQL(constraints []Constraint) (string, error) {
	constraintsSQL := make([]string, len(constraints))
	for i, c := range constraints {
		writer := &ConstraintSQLWriter{
			Name:              c.Name,
			Columns:           c.Columns,
			InitiallyDeferred: c.InitiallyDeferred,
			Deferrable:        c.Deferrable,
			IncludeColumns:    c.IndexParameters.IncludeColumns,
			StorageParameters: c.IndexParameters.StorageParameters,
			Tablespace:        c.IndexParameters.Tablespace,
		}

		switch c.Type { //nolint:gocritic // more cases are coming soon
		case ConstraintTypeUnique:
			constraintsSQL[i] = writer.WriteUnique(c.NullsNotDistinct)
		}
	}
	if len(constraintsSQL) == 0 {
		return "", nil
	}
	return ", " + strings.Join(constraintsSQL, ", "), nil
}

type ConstraintSQLWriter struct {
	Name              string
	Columns           []string
	InitiallyDeferred bool
	Deferrable        bool

	// unique, exclude, primary key constraints support the following options
	IncludeColumns    []string
	StorageParameters string
	Tablespace        string
}

func (w *ConstraintSQLWriter) WriteUnique(nullsNotDistinct bool) string {
	var constraint string
	if w.Name != "" {
		constraint = fmt.Sprintf("CONSTRAINT %s ", pq.QuoteIdentifier(w.Name))
	}
	nullsDistinct := ""
	if nullsNotDistinct {
		nullsDistinct = "NULLS NOT DISTINCT"
	}
	constraint += fmt.Sprintf("UNIQUE %s (%s)", nullsDistinct, strings.Join(quoteColumnNames(w.Columns), ", "))
	constraint += w.addIndexParameters()
	constraint += w.addDeferrable()
	return constraint
}

func (w *ConstraintSQLWriter) addIndexParameters() string {
	constraint := ""
	if len(w.IncludeColumns) != 0 {
		constraint += fmt.Sprintf(" INCLUDE (%s)", strings.Join(quoteColumnNames(w.IncludeColumns), ", "))
	}
	if w.StorageParameters != "" {
		constraint += fmt.Sprintf(" WITH (%s)", w.StorageParameters)
	}
	if w.Tablespace != "" {
		constraint += fmt.Sprintf(" USING INDEX TABLESPACE %s", w.Tablespace)
	}
	return constraint
}

func (w *ConstraintSQLWriter) addDeferrable() string {
	if !w.InitiallyDeferred && !w.Deferrable {
		return ""
	}
	deferrable := ""
	if w.Deferrable {
		deferrable += " DEFERRABLE"
	} else {
		deferrable += " NOT DEFERRABLE"
	}
	if w.InitiallyDeferred {
		deferrable += " INITIALLY DEFERRED"
	} else {
		deferrable += " INITIALLY IMMEDIATE"
	}
	return deferrable
}
