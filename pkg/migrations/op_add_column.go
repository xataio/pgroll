// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/xataio/pgroll/internal/defaults"
	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var (
	_ Operation  = (*OpAddColumn)(nil)
	_ Createable = (*OpAddColumn)(nil)
)

func (o *OpAddColumn) Start(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) (*StartResult, error) {
	l.LogOperationStart(o)

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

	action, err := addColumn(conn, *o, table, fastPathDefault)
	if err != nil {
		return nil, err
	}
	dbActions := []DBAction{action}

	if o.Column.Comment != nil {
		dbActions = append(dbActions, NewCommentColumnAction(conn, table.Name, TemporaryName(o.Column.Name), o.Column.Comment))
	}

	// If the column is `NOT NULL` and there is no default value (either because
	// the column as no DEFAULT or because the default value cannot be set using
	// the fast path optimization), add a NOT NULL constraint to the column which
	// will be validated on migration completion.
	skipInherit := false
	skipValidate := true
	if !o.Column.IsNullable() && (o.Column.Default == nil || !fastPathDefault) {
		dbActions = append(dbActions,
			NewCreateCheckConstraintAction(
				conn,
				table.Name,
				NotNullConstraintName(o.Column.Name),
				fmt.Sprintf("%s IS NOT NULL", o.Column.Name),
				[]string{o.Column.Name},
				skipInherit,
				skipValidate,
			))
	}

	if o.Column.Check != nil {
		dbActions = append(dbActions,
			NewCreateCheckConstraintAction(
				conn,
				table.Name,
				o.Column.Check.Name,
				o.Column.Check.Constraint,
				[]string{o.Column.Name},
				skipInherit,
				skipValidate,
			))
	}

	if o.Column.Unique {
		dbActions = append(dbActions,
			NewCreateUniqueIndexConcurrentlyAction(
				conn,
				s.Name,
				UniqueIndexName(o.Column.Name),
				table.Name,
				TemporaryName(o.Column.Name),
			))
	}

	// If the column has a DEFAULT that cannot be set using the fast path
	// optimization, the `up` SQL expression must be used to set the DEFAULT
	// value for the column.
	if o.Column.HasDefault() && !fastPathDefault {
		if o.Up != *o.Column.Default {
			return nil, UpSQLMustBeColumnDefaultError{Column: o.Column.Name}
		}
	}

	var task *backfill.Task
	if o.Up != "" {
		task = backfill.NewTask(table,
			backfill.OperationTrigger{
				Name:           backfill.TriggerName(o.Table, o.Column.Name),
				Direction:      backfill.TriggerDirectionUp,
				Columns:        table.Columns,
				TableName:      table.Name,
				PhysicalColumn: TemporaryName(o.Column.Name),
				SQL:            o.Up,
			},
		)
	}

	tmpColumn := toSchemaColumn(o.Column)
	tmpColumn.Name = TemporaryName(o.Column.Name)
	table.AddColumn(o.Column.Name, tmpColumn)

	return &StartResult{Actions: dbActions, BackfillTask: task}, nil
}

func toSchemaColumn(c Column) *schema.Column {
	tmpColumn := &schema.Column{
		Name:     c.Name,
		Type:     c.Type,
		Default:  c.Default,
		Nullable: c.Nullable,
		Unique:   c.Unique,
	}
	if c.Comment != nil {
		tmpColumn.Comment = *c.Comment
	}
	return tmpColumn
}

func (o *OpAddColumn) Complete(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationComplete(o)

	dbActions := []DBAction{
		NewRenameColumnAction(conn, o.Table, TemporaryName(o.Column.Name), o.Column.Name),
		NewDropFunctionAction(conn, backfill.TriggerFunctionName(o.Table, o.Column.Name)),
		NewDropColumnAction(conn, o.Table, backfill.CNeedsBackfillColumn),
	}

	if !o.Column.IsNullable() && o.Column.Default == nil {
		dbActions = append(dbActions, upgradeNotNullConstraintToNotNullAttribute(conn, o.Table, o.Column.Name)...)
	}

	if o.Column.Check != nil {
		dbActions = append(dbActions, NewValidateConstraintAction(conn, o.Table, o.Column.Check.Name))
	}

	if o.Column.Unique {
		dbActions = append(dbActions, NewAddConstraintUsingUniqueIndex(conn,
			o.Table,
			o.Column.Name,
			UniqueIndexName(o.Column.Name)))
	}

	// If the column has a DEFAULT that could not be set using the fast-path
	// optimization, set it here.
	column := s.GetTable(o.Table).GetColumn(TemporaryName(o.Column.Name))
	if o.Column.HasDefault() && column.Default == nil {
		dbActions = append(dbActions, NewSetDefaultValueAction(conn, o.Table, o.Column.Name, *o.Column.Default))

		// Validate the `NOT NULL` constraint on the column if necessary
		if !o.Column.IsNullable() {
			dbActions = append(dbActions, upgradeNotNullConstraintToNotNullAttribute(conn, o.Table, o.Column.Name)...)
		}
	}

	return dbActions, nil
}

func (o *OpAddColumn) Rollback(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationRollback(o)

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}
	column := table.GetColumn(o.Column.Name)
	if column == nil {
		return nil, ColumnDoesNotExistError{Table: o.Table, Name: o.Column.Name}
	}

	return []DBAction{
		NewDropColumnAction(conn, table.Name, column.Name),
		NewDropFunctionAction(conn, backfill.TriggerFunctionName(o.Table, o.Column.Name)),
		NewDropColumnAction(conn, table.Name, backfill.CNeedsBackfillColumn),
	}, nil
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

func addColumn(conn db.DB, o OpAddColumn, t *schema.Table, fastPathDefault bool) (DBAction, error) {
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
		return nil, fmt.Errorf("adding generated columns to existing tables is not supported")
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

	withPK := true
	return NewAddColumnAction(conn, t.Name, o.Column, withPK), nil
}

// upgradeNotNullConstraintToNotNullAttribute validates and upgrades a NOT NULL
// constraint to a NOT NULL column attribute. The constraint is removed after
// the column attribute is added.
func upgradeNotNullConstraintToNotNullAttribute(conn db.DB, tableName, columnName string) []DBAction {
	return []DBAction{
		NewValidateConstraintAction(conn, tableName, NotNullConstraintName(columnName)),
		NewSetNotNullAction(conn, tableName, columnName),
		NewDropConstraintAction(conn, tableName, NotNullConstraintName(columnName)),
	}
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
