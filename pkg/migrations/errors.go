// SPDX-License-Identifier: Apache-2.0

package migrations

import "fmt"

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
	Err    error
}

func (e CheckConstraintError) Unwrap() error {
	return e.Err
}

func (e CheckConstraintError) Error() string {
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

type NoUpSQLAllowedError struct{}

func (e NoUpSQLAllowedError) Error() string {
	return "up SQL is not allowed for this operation"
}

type NoDownSQLAllowedError struct{}

func (e NoDownSQLAllowedError) Error() string {
	return "down SQL is not allowed for this operation"
}

type MultipleAlterColumnChangesError struct {
	Changes int
}

func (e MultipleAlterColumnChangesError) Error() string {
	return fmt.Sprintf("alter column operations require exactly one change, found %d", e.Changes)
}

type InvalidPrimaryKeyError struct {
	Table  string
	Fields int
}

func (e InvalidPrimaryKeyError) Error() string {
	return fmt.Sprintf("primary key on table %q must be defined on exactly one column, found %d", e.Table, e.Fields)
}

type InvalidReplicaIdentityError struct {
	Table    string
	Identity string
}

func (e InvalidReplicaIdentityError) Error() string {
	return fmt.Sprintf("replica identity on table %q must be one of 'NOTHING', 'DEFAULT', 'INDEX' or 'FULL', found %q", e.Table, e.Identity)
}

type InvalidNameLengthError struct {
	Name string
	Max      int
}

func (e InvalidNameLengthError) Error() string {
	return fmt.Sprintf("max length of %s is %d", e.Identity, e.Max)
}
