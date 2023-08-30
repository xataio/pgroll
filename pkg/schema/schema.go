package schema

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
)

// XXX we create a view of the schema with the minimum required for us to
// know how to execute migrations and build views for the new schema version.
// As of now this is just the table names and column names.
// We should keep increasing the amount of information we store in the schema
// as we add more features.

func New() *Schema {
	return &Schema{
		Tables: make(map[string]Table),
	}
}

type Schema struct {
	// Name is the name of the schema
	Name string `json:"name"`
	// Tables is a map of virtual table name -> table mapping
	Tables map[string]Table `json:"tables"`
}

type Table struct {
	// OID for the table
	OID string `json:"oid"`

	// Name is the actual name in postgres
	Name string `json:"name"`

	// Optional comment for the table
	Comment string `json:"comment"`

	// Columns is a map of virtual column name -> column mapping
	Columns map[string]Column `json:"columns"`

	// Indexes is a map of the indexes defined on the table
	Indexes map[string]Index `json:"indexes"`
}

type Column struct {
	// Name is the actual name in postgres
	Name string `json:"name"`

	// Column type
	Type string `json:"type"`

	Default  *string `json:"default"`
	Nullable bool    `json:"nullable"`

	// Optional comment for the column
	Comment string `json:"comment"`
}

type Index struct {
	// Name is the name of the index in postgres
	Name string `json:"name"`
}

// Replace replaces the contents of the schema with the contents of the given one
func (s *Schema) Replace(other *Schema) {
	s.Tables = other.Tables
}

func (s *Schema) GetTable(name string) *Table {
	if s.Tables == nil {
		return nil
	}
	t, ok := s.Tables[name]
	if !ok {
		return nil
	}
	return &t
}

func (s *Schema) AddTable(name string, t Table) {
	if s.Tables == nil {
		s.Tables = make(map[string]Table)
	}

	s.Tables[name] = t
}

func (s *Schema) RenameTable(from, to string) error {
	if s.GetTable(from) == nil {
		return fmt.Errorf("table %q does not exist", from)
	}
	if s.GetTable(to) != nil {
		return fmt.Errorf("table %q already exists", to)
	}

	t := s.Tables[from]
	s.Tables[to] = t
	delete(s.Tables, from)
	return nil
}

func (s *Schema) RemoveTable(name string) {
	delete(s.Tables, name)
}

func (t *Table) GetColumn(name string) *Column {
	if t.Columns == nil {
		return nil
	}
	c, ok := t.Columns[name]
	if !ok {
		return nil
	}
	return &c
}

func (t *Table) AddColumn(name string, c Column) {
	if t.Columns == nil {
		t.Columns = make(map[string]Column)
	}

	t.Columns[name] = c
}

func (t *Table) RemoveColumn(column string) {
	delete(t.Columns, column)
}

func (t *Table) RenameColumn(from, to string) {
	t.Columns[to] = t.Columns[from]
	delete(t.Columns, from)
}

// Make the Schema struct implement the driver.Valuer interface. This method
// simply returns the JSON-encoded representation of the struct.
func (s Schema) Value() (driver.Value, error) {
	return json.Marshal(s)
}

// Make the Schema struct implement the sql.Scanner interface. This method
// simply decodes a JSON-encoded value into the struct fields.
func (s *Schema) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, &s)
}
