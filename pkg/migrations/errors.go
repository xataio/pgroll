package migrations

import "fmt"

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
