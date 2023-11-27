// Code generated by github.com/atombender/go-jsonschema, DO NOT EDIT.

package migrations

type CheckConstraint struct {
	// Constraint corresponds to the JSON schema field "constraint".
	Constraint string `json:"constraint"`

	// Name corresponds to the JSON schema field "name".
	Name string `json:"name"`
}

type Column struct {
	// Check corresponds to the JSON schema field "check".
	Check *CheckConstraint `json:"check,omitempty"`

	// Default corresponds to the JSON schema field "default".
	Default *string `json:"default,omitempty"`

	// Name corresponds to the JSON schema field "name".
	Name string `json:"name"`

	// Nullable corresponds to the JSON schema field "nullable".
	Nullable bool `json:"nullable"`

	// Pk corresponds to the JSON schema field "pk".
	Pk bool `json:"pk"`

	// References corresponds to the JSON schema field "references".
	References *ForeignKeyReference `json:"references,omitempty"`

	// Type corresponds to the JSON schema field "type".
	Type string `json:"type"`

	// Unique corresponds to the JSON schema field "unique".
	Unique bool `json:"unique"`
}

type ForeignKeyReference struct {
	// Column corresponds to the JSON schema field "column".
	Column string `json:"column"`

	// Name corresponds to the JSON schema field "name".
	Name string `json:"name"`

	// Table corresponds to the JSON schema field "table".
	Table string `json:"table"`
}

type OpAddColumn struct {
	// Column corresponds to the JSON schema field "column".
	Column Column `json:"column"`

	// Table corresponds to the JSON schema field "table".
	Table string `json:"table"`

	// Up corresponds to the JSON schema field "up".
	Up *string `json:"up,omitempty"`
}

type OpAlterColumn struct {
	// Check corresponds to the JSON schema field "check".
	Check *CheckConstraint `json:"check,omitempty"`

	// Column corresponds to the JSON schema field "column".
	Column string `json:"column"`

	// Down corresponds to the JSON schema field "down".
	Down string `json:"down"`

	// Name corresponds to the JSON schema field "name".
	Name string `json:"name"`

	// Nullable corresponds to the JSON schema field "nullable".
	Nullable *bool `json:"nullable,omitempty"`

	// References corresponds to the JSON schema field "references".
	References *ForeignKeyReference `json:"references,omitempty"`

	// Table corresponds to the JSON schema field "table".
	Table string `json:"table"`

	// Type corresponds to the JSON schema field "type".
	Type string `json:"type"`

	// Unique corresponds to the JSON schema field "unique".
	Unique *UniqueConstraint `json:"unique,omitempty"`

	// Up corresponds to the JSON schema field "up".
	Up string `json:"up"`
}

type OpCreateIndex struct {
	// Columns corresponds to the JSON schema field "columns".
	Columns []string `json:"columns"`

	// Name corresponds to the JSON schema field "name".
	Name string `json:"name"`

	// Table corresponds to the JSON schema field "table".
	Table string `json:"table"`
}

type OpCreateTable struct {
	// Columns corresponds to the JSON schema field "columns".
	Columns []Column `json:"columns"`

	// Name corresponds to the JSON schema field "name".
	Name string `json:"name"`
}

type OpDropColumn struct {
	// Column corresponds to the JSON schema field "column".
	Column string `json:"column"`

	// Down corresponds to the JSON schema field "down".
	Down *string `json:"down,omitempty"`

	// Table corresponds to the JSON schema field "table".
	Table string `json:"table"`
}

type OpDropConstraint struct {
	// Column corresponds to the JSON schema field "column".
	Column string `json:"column"`

	// Down corresponds to the JSON schema field "down".
	Down string `json:"down"`

	// Name corresponds to the JSON schema field "name".
	Name string `json:"name"`

	// Table corresponds to the JSON schema field "table".
	Table string `json:"table"`

	// Up corresponds to the JSON schema field "up".
	Up string `json:"up"`
}

type OpDropIndex struct {
	// Name corresponds to the JSON schema field "name".
	Name string `json:"name"`
}

type OpDropTable struct {
	// Name corresponds to the JSON schema field "name".
	Name string `json:"name"`
}

type OpRawSQL struct {
	// Down corresponds to the JSON schema field "down".
	Down string `json:"down,omitempty"`

	// Up corresponds to the JSON schema field "up".
	Up string `json:"up"`
}

type OpRenameTable struct {
	// From corresponds to the JSON schema field "from".
	From string `json:"from"`

	// To corresponds to the JSON schema field "to".
	To string `json:"to"`
}

type OpSetReplicaIdentity struct {
	// Identity corresponds to the JSON schema field "identity".
	Identity ReplicaIdentity `json:"identity"`

	// Table corresponds to the JSON schema field "table".
	Table string `json:"table"`
}

type PgRollMigration struct {
	// Name corresponds to the JSON schema field "name".
	Name string `json:"name"`

	// Operations corresponds to the JSON schema field "operations".
	Operations []interface{} `json:"operations"`
}

type ReplicaIdentity struct {
	// Index corresponds to the JSON schema field "Index".
	Index string `json:"Index"`

	// Type corresponds to the JSON schema field "Type".
	Type string `json:"Type"`
}

type UniqueConstraint struct {
	// Name corresponds to the JSON schema field "name".
	Name string `json:"name"`
}
