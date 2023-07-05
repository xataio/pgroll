package schema

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
	// Tables is a map of virtual table name -> table mapping
	Tables map[string]Table `json:"tables"`
}

func (s *Schema) AddTable(name string, t Table) {
	if s.Tables == nil {
		s.Tables = make(map[string]Table)
	}

	s.Tables[name] = t
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
