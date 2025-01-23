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

// Duplicator duplicates a column in a table, including all constraints and
// comments.
type Duplicator struct {
	stmtBuilder       *duplicatorStmtBuilder
	conn              db.DB
	columns           map[string]*columnToDuplicate
	withoutConstraint []string
}

type columnToDuplicate struct {
	column         *schema.Column
	asName         string
	withoutNotNull bool
	withType       string
}

// duplicatorStmtBuilder is a helper for building SQL statements to duplicate
// columns and constraints in a table.
type duplicatorStmtBuilder struct {
	table *schema.Table
}

const (
	dataTypeMismatchErrorCode  pq.ErrorCode = "42804"
	undefinedFunctionErrorCode pq.ErrorCode = "42883"

	cSetDefaultSQL                   = `ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s`
	cAlterTableAddCheckConstraintSQL = `ALTER TABLE %s ADD CONSTRAINT %s %s NOT VALID`
	cAlterTableAddForeignKeySQL      = `ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON DELETE %s`
)

// NewColumnDuplicator creates a new Duplicator for a column.
func NewColumnDuplicator(conn db.DB, table *schema.Table, columns ...*schema.Column) *Duplicator {
	cols := make(map[string]*columnToDuplicate, len(columns))
	for _, column := range columns {
		cols[column.Name] = &columnToDuplicate{
			column:   column,
			asName:   TemporaryName(column.Name),
			withType: column.Type,
		}
	}
	return &Duplicator{
		stmtBuilder: &duplicatorStmtBuilder{
			table: table,
		},
		conn:              conn,
		columns:           cols,
		withoutConstraint: make([]string, 0),
	}
}

// WithType sets the type of the new column.
func (d *Duplicator) WithType(columnName, t string) *Duplicator {
	d.columns[columnName].withType = t
	return d
}

// WithoutConstraint excludes a constraint from being duplicated.
func (d *Duplicator) WithoutConstraint(c string) *Duplicator {
	d.withoutConstraint = append(d.withoutConstraint, c)
	return d
}

// WithoutNotNull excludes the NOT NULL constraint from being duplicated.
func (d *Duplicator) WithoutNotNull(columnName string) *Duplicator {
	d.columns[columnName].withoutNotNull = true
	return d
}

// WithName sets the name of the new column.
func (d *Duplicator) WithName(columnName, asName string) *Duplicator {
	d.columns[columnName].asName = asName
	return d
}

// Duplicate duplicates a column in the table, including all constraints and
// comments.
func (d *Duplicator) Duplicate(ctx context.Context) error {
	colNames := make([]string, 0, len(d.columns))
	for name, c := range d.columns {
		colNames = append(colNames, name)

		// Duplicate the column with the new type
		if sql := d.stmtBuilder.duplicateColumn(c.column, c.asName, c.withoutNotNull, c.withType); sql != "" {
			_, err := d.conn.ExecContext(ctx, sql)
			if err != nil {
				return err
			}
		}

		// Duplicate the column's default value
		if sql := d.stmtBuilder.duplicateDefault(c.column, c.asName); sql != "" {
			_, err := d.conn.ExecContext(ctx, sql)
			err = errorIgnoringErrorCode(err, dataTypeMismatchErrorCode)
			if err != nil {
				return err
			}
		}

		// Duplicate the column's comment
		if sql := d.stmtBuilder.duplicateComment(c.column, c.asName); sql != "" {
			_, err := d.conn.ExecContext(ctx, sql)
			if err != nil {
				return err
			}
		}
	}

	// Generate SQL to duplicate any check constraints on the columns. This may faile
	// if the check constraint is not valid for the new column type, in which case
	// the error is ignored.
	for _, sql := range d.stmtBuilder.duplicateCheckConstraints(d.withoutConstraint, colNames...) {
		_, err := d.conn.ExecContext(ctx, sql)
		err = errorIgnoringErrorCode(err, undefinedFunctionErrorCode)
		if err != nil {
			return err
		}
	}

	// Create indexes for unique constraints on the columns concurrently.
	// The index is converted into a unique constraint on migration completion.
	for _, uc := range d.stmtBuilder.table.UniqueConstraints {
		if slices.Contains(d.withoutConstraint, uc.Name) {
			continue
		}
		if duplicatedMember, constraintColumns := d.stmtBuilder.allConstraintColumns(uc.Columns, colNames...); duplicatedMember {
			if err := createUniqueIndexConcurrently(ctx, d.conn, "", DuplicationName(uc.Name), d.stmtBuilder.table.Name, constraintColumns); err != nil {
				return err
			}
		}
	}

	// Generate SQL to duplicate any foreign key constraints on the columns.
	// If the foreign key constraint is not valid for a new column type, the error is ignored.
	for _, sql := range d.stmtBuilder.duplicateForeignKeyConstraints(d.withoutConstraint, colNames...) {
		_, err := d.conn.ExecContext(ctx, sql)
		err = errorIgnoringErrorCode(err, dataTypeMismatchErrorCode)
		if err != nil {
			return err
		}
	}

	// Generate SQL to duplicate any indexes on the columns.
	for _, sql := range d.stmtBuilder.duplicateIndexes(d.withoutConstraint, colNames...) {
		if _, err := d.conn.ExecContext(ctx, sql); err != nil {
			return err
		}
	}

	return nil
}

func (d *duplicatorStmtBuilder) duplicateCheckConstraints(withoutConstraint []string, colNames ...string) []string {
	stmts := make([]string, 0, len(d.table.CheckConstraints))
	for _, cc := range d.table.CheckConstraints {
		if slices.Contains(withoutConstraint, cc.Name) {
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

func (d *duplicatorStmtBuilder) duplicateForeignKeyConstraints(withoutConstraint []string, colNames ...string) []string {
	stmts := make([]string, 0, len(d.table.ForeignKeys))
	for _, fk := range d.table.ForeignKeys {
		if slices.Contains(withoutConstraint, fk.Name) {
			continue
		}
		if duplicatedMember, constraintColumns := d.allConstraintColumns(fk.Columns, colNames...); duplicatedMember {
			stmts = append(stmts, fmt.Sprintf(cAlterTableAddForeignKeySQL,
				pq.QuoteIdentifier(d.table.Name),
				pq.QuoteIdentifier(DuplicationName(fk.Name)),
				strings.Join(quoteColumnNames(constraintColumns), ", "),
				pq.QuoteIdentifier(fk.ReferencedTable),
				strings.Join(quoteColumnNames(fk.ReferencedColumns), ", "),
				fk.OnDelete,
			))
		}
	}
	return stmts
}

func (d *duplicatorStmtBuilder) duplicateIndexes(withoutConstraint []string, colNames ...string) []string {
	stmts := make([]string, 0, len(d.table.Indexes))
	for _, idx := range d.table.Indexes {
		if slices.Contains(withoutConstraint, idx.Name) {
			continue
		}
		if _, ok := d.table.UniqueConstraints[idx.Name]; ok && idx.Unique {
			// unique constraints are duplicated as unique indexes
			continue
		}

		if duplicatedMember, columns := d.allConstraintColumns(idx.Columns, colNames...); duplicatedMember {
			stmtFmt := "CREATE INDEX CONCURRENTLY %s ON %s"
			if idx.Unique {
				stmtFmt = "CREATE UNIQUE INDEX CONCURRENTLY %s ON %s"
			}
			stmt := fmt.Sprintf(stmtFmt, pq.QuoteIdentifier(DuplicationName(idx.Name)), pq.QuoteIdentifier(d.table.Name))
			if idx.Method != "" {
				stmt += fmt.Sprintf(" USING %s", string(idx.Method))
			}

			stmt += fmt.Sprintf(" (%s)", strings.Join(quoteColumnNames(columns), ", "))

			if storageParamStart := strings.Index(idx.Definition, " WITH ("); storageParamStart != -1 {
				end := strings.Index(idx.Definition[storageParamStart:], ")")
				stmt += idx.Definition[storageParamStart : storageParamStart+end+1]
			}

			if idx.Predicate != nil {
				pred := strings.Replace(*idx.Predicate, strings.Join(idx.Columns, ", "), strings.Join(quoteColumnNames(columns), ", "), 1)
				stmt += fmt.Sprintf(" WHERE %s", pred)
			}

			stmts = append(stmts, stmt)
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
) string {
	const (
		cAlterTableSQL         = `ALTER TABLE %s ADD COLUMN %s %s`
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

// DuplicationName returns the name of a duplicated column.
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

func errorIgnoringErrorCode(err error, code pq.ErrorCode) error {
	pqErr := &pq.Error{}
	if ok := errors.As(err, &pqErr); ok {
		if pqErr.Code == code {
			return nil
		}
	}

	return err
}
