// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

// ColumnDuplicator duplicates a column in a table, including all constraints and
// comments.
type ColumnDuplicator struct {
	duplicator        *duplicatorStmtBuilder
	conn              db.DB
	column            *schema.Column
	asName            string
	withoutNotNull    bool
	withType          string
	withoutConstraint string
}

// ColumnGroupDuplicator duplicates a group of columns in a table, including all constraints and
// comments.
type ColumnGroupDuplicator struct {
	duplicator *duplicatorStmtBuilder
	conn       db.DB
	columns    []*schema.Column
}

// duplicatorStmtBuilder is a helper for building SQL statements to duplicate
// columns and constraints in a table.
type duplicatorStmtBuilder struct {
	table *schema.Table
}

const (
	dataTypeMismatchErrorCode  pq.ErrorCode = "42804"
	undefinedFunctionErrorCode pq.ErrorCode = "42883"

	cCreateUniqueIndexSQL            = `CREATE UNIQUE INDEX CONCURRENTLY %s ON %s (%s)`
	cSetDefaultSQL                   = `ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s`
	cAlterTableAddCheckConstraintSQL = `ALTER TABLE %s ADD CONSTRAINT %s %s NOT VALID`
)

// NewColumnDuplicator creates a new Duplicator for a column.
func NewColumnDuplicator(conn db.DB, table *schema.Table, column *schema.Column) *ColumnDuplicator {
	return &ColumnDuplicator{
		duplicator: &duplicatorStmtBuilder{
			table: table,
		},
		conn:     conn,
		column:   column,
		asName:   TemporaryName(column.Name),
		withType: column.Type,
	}
}

func NewColumnGroupDuplicator(conn db.DB, table *schema.Table, columns []*schema.Column) *ColumnGroupDuplicator {
	return &ColumnGroupDuplicator{
		duplicator: &duplicatorStmtBuilder{
			table: table,
		},
		conn:    conn,
		columns: columns,
	}
}

// WithType sets the type of the new column.
func (d *ColumnDuplicator) WithType(t string) *ColumnDuplicator {
	d.withType = t
	return d
}

// WithoutConstraint excludes a constraint from being duplicated.
func (d *ColumnDuplicator) WithoutConstraint(c string) *ColumnDuplicator {
	d.withoutConstraint = c
	return d
}

// WithoutNotNull excludes the NOT NULL constraint from being duplicated.
func (d *ColumnDuplicator) WithoutNotNull() *ColumnDuplicator {
	d.withoutNotNull = true
	return d
}

// Duplicate duplicates a column in the table, including all constraints and
// comments.
func (d *ColumnDuplicator) Duplicate(ctx context.Context) error {
	// Duplicate the column with the new type
	// and check and fk constraints
	if sql := d.duplicator.duplicateColumn(d.column, d.asName, d.withoutNotNull, d.withType, d.withoutConstraint); sql != "" {
		_, err := d.conn.ExecContext(ctx, sql)
		if err != nil {
			return err
		}
	}

	// Duplicate the column's default value
	if sql := d.duplicator.duplicateDefault(d.column, d.asName); sql != "" {
		_, err := d.conn.ExecContext(ctx, sql)
		err = errorIgnoringErrorCode(err, dataTypeMismatchErrorCode)
		if err != nil {
			return err
		}
	}

	if sql := d.duplicator.duplicateComment(d.column, d.asName); sql != "" {
		_, err := d.conn.ExecContext(ctx, sql)
		if err != nil {
			return err
		}
	}

	// Generate SQL to duplicate any check constraints on the column. This may faile
	// if the check constraint is not valid for the new column type, in which case
	// the error is ignored.
	for _, sql := range d.duplicator.duplicateCheckConstraints(d.withoutConstraint, d.column.Name) {
		// Update the check constraint expression to use the new column names if any of the columns are duplicated
		_, err := d.conn.ExecContext(ctx, sql)
		err = errorIgnoringErrorCode(err, undefinedFunctionErrorCode)
		if err != nil {
			return err
		}
	}

	// Generate SQL to duplicate any unique constraints on the column
	// The constraint is duplicated by adding a unique index on the column concurrently.
	// The index is converted into a unique constraint on migration completion.
	for _, sql := range d.duplicator.duplicateUniqueConstraints(d.withoutConstraint, d.column.Name) {
		// Update the unique constraint columns to use the new column names if any of the columns are duplicated
		if _, err := d.conn.ExecContext(ctx, sql); err != nil {
			return err
		}
	}

	return nil
}

func (cg *ColumnGroupDuplicator) Duplicate(ctx context.Context) error {
	withoutConstraint := ""
	colNames := make([]string, len(cg.columns))
	for i, column := range cg.columns {
		colNames[i] = column.Name
		asName := TemporaryName(column.Name)
		withoutNotNull := false
		// Duplicate the column with the new type
		// and check and fk constraints
		if sql := cg.duplicator.duplicateColumn(column, asName, withoutNotNull, column.Type, withoutConstraint); sql != "" {
			_, err := cg.conn.ExecContext(ctx, sql)
			if err != nil {
				return err
			}
		}

		// Duplicate the column's default value
		if sql := cg.duplicator.duplicateDefault(column, asName); sql != "" {
			_, err := cg.conn.ExecContext(ctx, sql)
			if err != nil {
				return err
			}
		}

		// Duplicate the column's comment
		if sql := cg.duplicator.duplicateComment(column, asName); sql != "" {
			_, err := cg.conn.ExecContext(ctx, sql)
			if err != nil {
				return err
			}
		}
	}

	// Generate SQL to duplicate any check constraints on the column group.
	for _, sql := range cg.duplicator.duplicateCheckConstraints(withoutConstraint, colNames...) {
		// Update the check constraint expression to use the new column names if any of the columns are duplicated
		_, err := cg.conn.ExecContext(ctx, sql)
		err = errorIgnoringErrorCode(err, undefinedFunctionErrorCode)
		if err != nil {
			return err
		}
	}

	// Generate SQL to duplicate any unique constraints on the column group.
	for _, sql := range cg.duplicator.duplicateUniqueConstraints(withoutConstraint, colNames...) {
		// Update the unique constraint columns to use the new column names if any of the columns are duplicated
		if _, err := cg.conn.ExecContext(ctx, sql); err != nil {
			return err
		}
	}
	return nil
}

func (d *duplicatorStmtBuilder) duplicateCheckConstraints(withoutConstraint string, colNames ...string) []string {
	stmts := make([]string, 0, len(d.table.CheckConstraints))
	for _, cc := range d.table.CheckConstraints {
		if cc.Name == withoutConstraint {
			continue
		}
		if duplicatedConstraintColumns := d.duplicatedConstraintColumns(cc.Columns, colNames...); len(duplicatedConstraintColumns) > 0 {
			stmts = append(stmts, fmt.Sprintf(cAlterTableAddCheckConstraintSQL,
				pq.QuoteIdentifier(d.table.Name),
				pq.QuoteIdentifier(DuplicationName(cc.Name)),
				rewriteCheckExpression(cc.Definition, duplicatedConstraintColumns...),
			))
		}
	}
	return stmts
}

func (d *duplicatorStmtBuilder) duplicateUniqueConstraints(withoutConstraint string, colNames ...string) []string {
	stmts := make([]string, 0, len(d.table.UniqueConstraints))
	for _, uc := range d.table.UniqueConstraints {
		if uc.Name == withoutConstraint {
			continue
		}
		if duplicatedMember, constraintColumns := d.allConstraintColumns(uc.Columns, colNames...); duplicatedMember {
			stmts = append(stmts, fmt.Sprintf(cCreateUniqueIndexSQL,
				pq.QuoteIdentifier(DuplicationName(uc.Name)),
				pq.QuoteIdentifier(d.table.Name),
				strings.Join(quoteColumnNames(constraintColumns), ", "),
			))
		}
	}
	return stmts
}

// duplicatedConstraintColumns returns a new slice of constraint columns with
// the columns that are duplicated replaced with temporary names.
func (d *duplicatorStmtBuilder) duplicatedConstraintColumns(constraintColumns []string, duplicatedColumns ...string) []string {
	newConstraintColumns := make([]string, 0)
	for _, column := range constraintColumns {
		if slices.Contains(duplicatedColumns, column) {
			newConstraintColumns = append(newConstraintColumns, column)
		}
	}
	return newConstraintColumns
}

// allConstraintColumns returns a new slice of constraint columns with the columns
// that are duplicated replaced with temporary names and a boolean indicating if
// any of the columns are duplicated.
func (d *duplicatorStmtBuilder) allConstraintColumns(constraintColumns []string, duplicatedColumns ...string) (bool, []string) {
	duplicatedMember := false
	newConstraintColumns := make([]string, len(constraintColumns))
	for i, column := range constraintColumns {
		if slices.Contains(duplicatedColumns, column) {
			newConstraintColumns[i] = TemporaryName(column)
			duplicatedMember = true
		} else {
			newConstraintColumns[i] = column
		}
	}
	return duplicatedMember, newConstraintColumns
}

func (d *duplicatorStmtBuilder) duplicateColumn(
	column *schema.Column,
	asName string,
	withoutNotNull bool,
	withType string,
	withoutConstraint string,
) string {
	const (
		cAlterTableSQL         = `ALTER TABLE %s ADD COLUMN %s %s`
		cAddForeignKeySQL      = `ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON DELETE %s`
		cAddCheckConstraintSQL = `ADD CONSTRAINT %s %s NOT VALID`
	)

	// Generate SQL to duplicate the column's name and type
	sql := fmt.Sprintf(cAlterTableSQL,
		pq.QuoteIdentifier(d.table.Name),
		pq.QuoteIdentifier(asName),
		withType)

	// Generate SQL to add an unchecked NOT NULL constraint if the original column
	// is NOT NULL. The constraint will be validated on migration completion.
	if !column.Nullable && !withoutNotNull {
		sql += fmt.Sprintf(", "+cAddCheckConstraintSQL,
			pq.QuoteIdentifier(DuplicationName(NotNullConstraintName(column.Name))),
			fmt.Sprintf("CHECK (%s IS NOT NULL)", pq.QuoteIdentifier(asName)),
		)
	}

	// Generate SQL to duplicate any foreign key constraints on the column
	for _, fk := range d.table.ForeignKeys {
		if fk.Name == withoutConstraint {
			continue
		}

		if slices.Contains(fk.Columns, column.Name) {
			sql += fmt.Sprintf(", "+cAddForeignKeySQL,
				pq.QuoteIdentifier(DuplicationName(fk.Name)),
				strings.Join(quoteColumnNames(copyAndReplace(fk.Columns, column.Name, asName)), ", "),
				pq.QuoteIdentifier(fk.ReferencedTable),
				strings.Join(quoteColumnNames(fk.ReferencedColumns), ", "),
				fk.OnDelete,
			)
		}
	}

	return sql
}

func (d *duplicatorStmtBuilder) duplicateDefault(column *schema.Column, asName string) string {
	if column.Default == nil {
		return ""
	}

	const cSetDefaultSQL = `ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s`

	// Generate SQL to duplicate any default value on the column. This may fail
	// if the default value is not valid for the new column type, in which case
	// the error is ignored.
	return fmt.Sprintf(cSetDefaultSQL, pq.QuoteIdentifier(d.table.Name), asName, *column.Default)
}

func (d *duplicatorStmtBuilder) duplicateComment(column *schema.Column, asName string) string {
	if column.Comment == "" {
		return ""
	}

	const cCommentOnColumnSQL = `COMMENT ON COLUMN %s.%s IS %s`

	// Generate SQL to duplicate the column's comment
	return fmt.Sprintf(cCommentOnColumnSQL,
		pq.QuoteIdentifier(d.table.Name),
		pq.QuoteIdentifier(asName),
		pq.QuoteLiteral(column.Comment),
	)
}

// DiplicationName returns the name of a duplicated column.
func DuplicationName(name string) string {
	return "_pgroll_dup_" + name
}

// IsDuplicatedName returns true if the name is a duplicated column name.
func IsDuplicatedName(name string) bool {
	return strings.HasPrefix(name, "_pgroll_dup_")
}

// StripDuplicationPrefix removes the duplication prefix from a column name.
func StripDuplicationPrefix(name string) string {
	return strings.TrimPrefix(name, "_pgroll_dup_")
}

func copyAndReplace(xs []string, oldValue, newValue string) []string {
	ys := slices.Clone(xs)

	for i, c := range ys {
		if c == oldValue {
			ys[i] = newValue
		}
	}
	return ys
}

func errorIgnoringErrorCode(err error, code pq.ErrorCode) error {
	pqErr := &pq.Error{}
	if ok := errors.As(err, &pqErr); ok {
		if pqErr.Code == code {
			return nil
		}
	}

	return err
}
