// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"fmt"
)

type InvalidMigrationError struct {
	Reason string
}

func (e InvalidMigrationError) Error() string {
	return e.Reason
}

type EmptyMigrationError struct{}

func (e EmptyMigrationError) Error() string {
	return "migration is empty"
}

type TableAlreadyExistsError struct {
	Name string
}

func (e TableAlreadyExistsError) Error() string {
	return fmt.Sprintf("table %q already exists", e.Name)
}

type TableDoesNotExistError struct {
	Name string
}

func (e TableDoesNotExistError) Error() string {
	return fmt.Sprintf("table %q does not exist", e.Name)
}

type ColumnAlreadyExistsError struct {
	Table string
	Name  string
}

func (e ColumnAlreadyExistsError) Error() string {
	return fmt.Sprintf("column %q already exists in table %q", e.Name, e.Table)
}

type ColumnDoesNotExistError struct {
	Table string
	Name  string
}

func (e ColumnDoesNotExistError) Error() string {
	return fmt.Sprintf("column %q does not exist on table %q", e.Name, e.Table)
}

type ColumnMigrationMissingError struct {
	Table string
	Name  string
}

func (e ColumnMigrationMissingError) Error() string {
	return fmt.Sprintf("migration for column %q in %q is missing", e.Name, e.Table)
}

type ColumnMigrationRedundantError struct {
	Table string
	Name  string
}

func (e ColumnMigrationRedundantError) Error() string {
	return fmt.Sprintf("migration for column %q in %q is redundant", e.Name, e.Table)
}

type ColumnIsNotNullableError struct {
	Table string
	Name  string
}

func (e ColumnIsNotNullableError) Error() string {
	return fmt.Sprintf("column %q on table %q is NOT NULL", e.Name, e.Table)
}

type ColumnIsNullableError struct {
	Table string
	Name  string
}

func (e ColumnIsNullableError) Error() string {
	return fmt.Sprintf("column %q on table %q is nullable", e.Name, e.Table)
}

type IndexAlreadyExistsError struct {
	Name string
}

func (e IndexAlreadyExistsError) Error() string {
	return fmt.Sprintf("index %q already exists", e.Name)
}

type IndexDoesNotExistError struct {
	Name string
}

func (e IndexDoesNotExistError) Error() string {
	return fmt.Sprintf("index %q does not exist", e.Name)
}

type FieldRequiredError struct {
	Name string
}

func (e FieldRequiredError) Error() string {
	return fmt.Sprintf("field %q is required", e.Name)
}

type ColumnReferenceError struct {
	Table  string
	Column string
	Err    error
}

func (e ColumnReferenceError) Unwrap() error {
	return e.Err
}

func (e ColumnReferenceError) Error() string {
	return fmt.Sprintf("column reference to column %q in table %q is invalid: %s",
		e.Column,
		e.Table,
		e.Err.Error())
}

type CheckConstraintError struct {
	Table  string
	Column string
	Name   string
	Err    error
}

func (e CheckConstraintError) Unwrap() error {
	return e.Err
}

func (e CheckConstraintError) Error() string {
	if e.Column == "" {
		return fmt.Sprintf("check constraint %q in table %q is invalid: %s",
			e.Name,
			e.Table,
			e.Err.Error())
	}

	return fmt.Sprintf("check constraint on column %q in table %q is invalid: %s",
		e.Table,
		e.Column,
		e.Err.Error())
}

type ConstraintDoesNotExistError struct {
	Table      string
	Constraint string
}

func (e ConstraintDoesNotExistError) Error() string {
	return fmt.Sprintf("constraint %q on table %q does not exist", e.Constraint, e.Table)
}

type ConstraintAlreadyExistsError struct {
	Table      string
	Constraint string
}

func (e ConstraintAlreadyExistsError) Error() string {
	return fmt.Sprintf("constraint %q on table %q already exists", e.Constraint, e.Table)
}

type NoUpSQLAllowedError struct{}

func (e NoUpSQLAllowedError) Error() string {
	return "up SQL is not allowed for this operation"
}

type NoDownSQLAllowedError struct{}

func (e NoDownSQLAllowedError) Error() string {
	return "down SQL is not allowed for this operation"
}

type BackfillNotPossibleError struct {
	Table string
}

func (e BackfillNotPossibleError) Error() string {
	return fmt.Sprintf("a backfill is required but table %q doesn't have a single column primary key or a UNIQUE, NOT NULL column", e.Table)
}

type InvalidReplicaIdentityError struct {
	Table    string
	Identity string
}

func (e InvalidReplicaIdentityError) Error() string {
	return fmt.Sprintf("replica identity on table %q must be one of 'NOTHING', 'DEFAULT', 'INDEX' or 'FULL', found %q", e.Table, e.Identity)
}

type InvalidOnDeleteSettingError struct {
	Name    string
	Setting string
}

func (e InvalidOnDeleteSettingError) Error() string {
	return fmt.Sprintf("foreign key %q on_delete setting must be one of: %q, %q, %q, %q or %q, not %q",
		e.Name,
		ForeignKeyReferenceOnDeleteNOACTION,
		ForeignKeyReferenceOnDeleteRESTRICT,
		ForeignKeyReferenceOnDeleteSETDEFAULT,
		ForeignKeyReferenceOnDeleteSETNULL,
		ForeignKeyReferenceOnDeleteCASCADE,
		e.Setting,
	)
}

type AlterColumnNoChangesError struct {
	Table  string
	Column string
}

func (e AlterColumnNoChangesError) Error() string {
	return fmt.Sprintf("alter column %q on table %q requires at least one change", e.Column, e.Table)
}

// maxIdentifierLength is the maximum length of a valid identifier:
// https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
const maxIdentifierLength = 63

type InvalidIdentifierLengthError struct {
	Name string
}

func (e InvalidIdentifierLengthError) Error() string {
	return fmt.Sprintf("length of %q (%d) exceeds maximum length of %d", e.Name, len(e.Name), maxIdentifierLength)
}

// ValidateIdentifierLength returns an error if the given name exceeds the maximum allowed length for
// a Postgres identifier.
func ValidateIdentifierLength(name string) error {
	if len(name) > maxIdentifierLength {
		return InvalidIdentifierLengthError{Name: name}
	}
	return nil
}

type MultiColumnConstraintsNotSupportedError struct {
	Table      string
	Constraint string
}

func (e MultiColumnConstraintsNotSupportedError) Error() string {
	return fmt.Sprintf("constraint %q on table %q applies to multiple columns", e.Constraint, e.Table)
}
