package schema

// XXX we create a view of the schema with the minimum required for us to
// know how to execute migrations and build views for the new schema version.
// As of now this is just the table names and column names.

func New() *Schema {
	return &Schema{
		Tables: make(map[string]Table),
	}
}

type Schema struct {
	Tables map[string]Table // virtual name -> table mapping
}

type Table struct {
	Name    string            // actual name in postgres
	Columns map[string]Column // virtual name -> column mapping
}

type Column struct {
	Name string // actual name in postgres
}
