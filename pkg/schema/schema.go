// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

// XXX we create a view of the schema with the minimum required for us to
// know how to execute migrations and build views for the new schema version.
// As of now this is just the table names and column names.
// We should keep increasing the amount of information we store in the schema
// as we add more features.

func New() *Schema {
	return &Schema{
		Tables: make(map[string]*Table),
	}
}

// Schema represents a database schema
type Schema struct {
	// Name is the name of the schema
	Name string `json:"name"`
	// Tables is a map of virtual table name -> table mapping
	Tables map[string]*Table `json:"tables"`
}

// Table represents a table in the schema
type Table struct {
	// OID for the table
	OID string `json:"oid"`

	// Name is the actual name in postgres
	Name string `json:"name"`

	// Optional comment for the table
	Comment string `json:"comment"`

	// Columns is a map of virtual column name -> column mapping
	Columns map[string]*Column `json:"columns"`

	// Indexes is a map of the indexes defined on the table
	Indexes map[string]*Index `json:"indexes"`

	// The columns that make up the primary key
	PrimaryKey []string `json:"primaryKey"`

	// ForeignKeys is a map of all foreign keys defined on the table
	ForeignKeys map[string]*ForeignKey `json:"foreignKeys"`

	// CheckConstraints is a map of all check constraints defined on the table
	CheckConstraints map[string]*CheckConstraint `json:"checkConstraints"`

	// UniqueConstraints is a map of all unique constraints defined on the table
	UniqueConstraints map[string]*UniqueConstraint `json:"uniqueConstraints"`

	// ExcludeConstraints is a map of all exclude constraints defined on the table
	ExcludeConstraints map[string]*ExcludeConstraint `json:"excludeConstraints"`

	// Whether or not the table has been deleted in the virtual schema
	Deleted bool `json:"-"`
}

// Column represents a column in a table
type Column struct {
	// Name is the actual name in postgres
	Name string `json:"name"`

	// Column type
	Type string `json:"type"`

	Default  *string `json:"default"`
	Nullable bool    `json:"nullable"`
	Unique   bool    `json:"unique"`

	// Optional comment for the column
	Comment string `json:"comment"`

	// Will contain possible enum values if the type is an enum
	EnumValues []string `json:"enumValues"`

	// Whether or not the column has been deleted in the virtual schema
	Deleted bool `json:"-"`

	// Postgres type type, e.g enum, composite, range
	PostgresType string `json:"postgresType"`
}

// Index represents an index on a table
type Index struct {
	// Name is the name of the index in postgres
	Name string `json:"name"`

	// Unique indicates whether or not the index is unique
	Unique bool `json:"unique"`

	// Exclusion indicates whether or not the index is an exclusion index
	Exclusion bool `json:"exclusion"`

	// Columns is the set of key columns on which the index is defined
	Columns []string `json:"columns"`

	// Predicate is the optional predicate for the index
	Predicate *string `json:"predicate,omitempty"`

	// Method is the method for the index
	Method string `json:"method,omitempty"`

	// Definition is statement to construct the index
	Definition string `json:"definition"`
}

// ForeignKey represents a foreign key on a table
type ForeignKey struct {
	// Name is the name of the foreign key in postgres
	Name string `json:"name"`

	// The columns that the foreign key is defined on
	Columns []string `json:"columns"`

	// The table that the foreign key references
	ReferencedTable string `json:"referencedTable"`

	// The columns in the referenced table that the foreign key references
	ReferencedColumns []string `json:"referencedColumns"`

	// The ON DELETE behavior of the foreign key
	OnDelete string `json:"onDelete"`

	// The ON UPDATE behavior of the foreign key
	OnUpdate string `json:"onUpdate"`

	// MatchType is the match type of the foreign key
	MatchType string `json:"matchType"`
}

// CheckConstraint represents a check constraint on a table
type CheckConstraint struct {
	// Name is the name of the check constraint in postgres
	Name string `json:"name"`

	// The columns that the check constraint is defined on
	Columns []string `json:"columns"`

	// The definition of the check constraint
	Definition string `json:"definition"`
}

// UniqueConstraint represents a unique constraint on a table
type UniqueConstraint struct {
	// Name is the name of the unique constraint in postgres
	Name string `json:"name"`

	// The columns that the unique constraint is defined on
	Columns []string `json:"columns"`
}

// ExcludeConstraint represents a unique constraint on a table
type ExcludeConstraint struct {
	// Name is the name of the exclude constraint in postgres
	Name string `json:"name"`

	// Method is the index method of the exclude constraint
	Method string `json:"method"`

	// Predicate is the predicate of the index
	Predicate string `json:"predicate"`

	// The columns that the exclusion constraint is defined on
	Columns []string `json:"columns"`

	// The definition of the exclusion
	Definition string `json:"definition"`
}

// GetTable returns a table by name
func (s *Schema) GetTable(name string) *Table {
	if s.Tables == nil {
		return nil
	}
	t, ok := s.Tables[name]
	if !ok || t.Deleted {
		return nil
	}
	return t
}

// AddTable adds a table to the schema
func (s *Schema) AddTable(name string, t *Table) {
	if s.Tables == nil {
		s.Tables = make(map[string]*Table)
	}

	s.Tables[name] = t
}

// RenameTable renames a table in the schema
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

// RemoveTable removes a table from the schema by marking it as deleted
func (s *Schema) RemoveTable(name string) {
	if tbl, ok := s.Tables[name]; ok {
		tbl.Deleted = true
	}
}

// UnRemoveTable unremoves a previously removed table by marking it as not
// deleted
func (s *Schema) UnRemoveTable(name string) {
	if tbl, ok := s.Tables[name]; ok {
		tbl.Deleted = false
	}
}

// GetColumn returns a column by name
func (t *Table) GetColumn(name string) *Column {
	if t.Columns == nil {
		return nil
	}
	c, ok := t.Columns[name]
	if !ok || c.Deleted {
		return nil
	}
	return c
}

// ConstraintExists returns true if a constraint with the given name exists
func (t *Table) ConstraintExists(name string) bool {
	_, ok := t.CheckConstraints[name]
	if ok {
		return true
	}
	_, ok = t.UniqueConstraints[name]
	if ok {
		return true
	}
	_, ok = t.ForeignKeys[name]
	return ok
}

// GetConstraintColumns gets the columns associated with the given constraint. It may return a nil
// slice if the constraint does not exist.
func (t *Table) GetConstraintColumns(name string) []string {
	var columns []string
	if c, ok := t.CheckConstraints[name]; ok {
		columns = append(columns, c.Columns...)
	}
	if c, ok := t.UniqueConstraints[name]; ok {
		columns = append(columns, c.Columns...)
	}
	if c, ok := t.ForeignKeys[name]; ok {
		columns = append(columns, c.Columns...)
	}

	// Deduplicate and sort
	slices.Sort(columns)
	return slices.Compact(columns)
}

// GetPrimaryKey returns the columns that make up the primary key
func (t *Table) GetPrimaryKey() (columns []*Column) {
	for _, name := range t.PrimaryKey {
		columns = append(columns, t.GetColumn(name))
	}
	return columns
}

// AddColumn adds a column to the table
func (t *Table) AddColumn(name string, c *Column) {
	if t.Columns == nil {
		t.Columns = make(map[string]*Column)
	}

	t.Columns[name] = c
}

// RemoveColumn removes a column from the table by marking it as deleted
func (t *Table) RemoveColumn(column string) {
	if col, ok := t.Columns[column]; ok {
		col.Deleted = true
	}
}

// UnRemoveColumn unremoves a previously removed column by marking it as not
// deleted
func (t *Table) UnRemoveColumn(column string) {
	if col, ok := t.Columns[column]; ok {
		col.Deleted = false
	}
}

// RenameColumn renames a column in the table
func (t *Table) RenameColumn(from, to string) {
	t.Columns[to] = t.Columns[from]
	delete(t.Columns, from)
}

// PhysicalColumnNamesFor returns the physical column names for the given virtual
// column names
func (t *Table) PhysicalColumnNamesFor(columnNames ...string) []string {
	physicalNames := make([]string, 0, len(columnNames))
	for _, cn := range columnNames {
		physicalNames = append(physicalNames, t.GetColumn(cn).Name)
	}
	return physicalNames
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
