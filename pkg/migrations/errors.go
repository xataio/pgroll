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
