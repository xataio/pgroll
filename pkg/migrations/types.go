// Code generated by github.com/atombender/go-jsonschema, DO NOT EDIT.

package migrations

type CheckConstraint struct {
	// Constraint corresponds to the JSON schema field "constraint".
	Constraint string `json:"constraint" yaml:"constraint" mapstructure:"constraint"`

	// Name corresponds to the JSON schema field "name".
	Name string `json:"name" yaml:"name" mapstructure:"name"`
}

type Column struct {
	// Check corresponds to the JSON schema field "check".
	Check *CheckConstraint `json:"check,omitempty" yaml:"check,omitempty" mapstructure:"check,omitempty"`

	// Default corresponds to the JSON schema field "default".
	Default *string `json:"default,omitempty" yaml:"default,omitempty" mapstructure:"default,omitempty"`

	// Name corresponds to the JSON schema field "name".
	Name string `json:"name" yaml:"name" mapstructure:"name"`

	// Nullable corresponds to the JSON schema field "nullable".
	Nullable bool `json:"nullable" yaml:"nullable" mapstructure:"nullable"`

	// Pk corresponds to the JSON schema field "pk".
	Pk bool `json:"pk" yaml:"pk" mapstructure:"pk"`

	// References corresponds to the JSON schema field "references".
	References *ForeignKeyReference `json:"references,omitempty" yaml:"references,omitempty" mapstructure:"references,omitempty"`

	// Type corresponds to the JSON schema field "type".
	Type string `json:"type" yaml:"type" mapstructure:"type"`

	// Unique corresponds to the JSON schema field "unique".
	Unique bool `json:"unique" yaml:"unique" mapstructure:"unique"`
}

type ForeignKeyReference struct {
	// Column corresponds to the JSON schema field "column".
	Column string `json:"column" yaml:"column" mapstructure:"column"`

	// Name corresponds to the JSON schema field "name".
	Name string `json:"name" yaml:"name" mapstructure:"name"`

	// Table corresponds to the JSON schema field "table".
	Table string `json:"table" yaml:"table" mapstructure:"table"`
}

type OpAddColumn struct {
	// Column corresponds to the JSON schema field "column".
	Column Column `json:"column" yaml:"column" mapstructure:"column"`

	// Table corresponds to the JSON schema field "table".
	Table string `json:"table" yaml:"table" mapstructure:"table"`

	// Up corresponds to the JSON schema field "up".
	Up *string `json:"up,omitempty" yaml:"up,omitempty" mapstructure:"up,omitempty"`
}

type OpAlterColumn struct {
	// Check corresponds to the JSON schema field "check".
	Check *CheckConstraint `json:"check,omitempty" yaml:"check,omitempty" mapstructure:"check,omitempty"`

	// Column corresponds to the JSON schema field "column".
	Column string `json:"column" yaml:"column" mapstructure:"column"`

	// Down corresponds to the JSON schema field "down".
	Down string `json:"down" yaml:"down" mapstructure:"down"`

	// Name corresponds to the JSON schema field "name".
	Name string `json:"name" yaml:"name" mapstructure:"name"`

	// Nullable corresponds to the JSON schema field "nullable".
	Nullable *bool `json:"nullable,omitempty" yaml:"nullable,omitempty" mapstructure:"nullable,omitempty"`

	// References corresponds to the JSON schema field "references".
	References *ForeignKeyReference `json:"references,omitempty" yaml:"references,omitempty" mapstructure:"references,omitempty"`

	// Table corresponds to the JSON schema field "table".
	Table string `json:"table" yaml:"table" mapstructure:"table"`

	// Type corresponds to the JSON schema field "type".
	Type string `json:"type" yaml:"type" mapstructure:"type"`

	// Unique corresponds to the JSON schema field "unique".
	Unique *UniqueConstraint `json:"unique,omitempty" yaml:"unique,omitempty" mapstructure:"unique,omitempty"`

	// Up corresponds to the JSON schema field "up".
	Up string `json:"up" yaml:"up" mapstructure:"up"`
}

type OpCreateIndex struct {
	// Columns corresponds to the JSON schema field "columns".
	Columns []string `json:"columns" yaml:"columns" mapstructure:"columns"`

	// Name corresponds to the JSON schema field "name".
	Name string `json:"name" yaml:"name" mapstructure:"name"`

	// Table corresponds to the JSON schema field "table".
	Table string `json:"table" yaml:"table" mapstructure:"table"`
}

type OpCreateTable struct {
	// Columns corresponds to the JSON schema field "columns".
	Columns []Column `json:"columns" yaml:"columns" mapstructure:"columns"`

	// Name corresponds to the JSON schema field "name".
	Name string `json:"name" yaml:"name" mapstructure:"name"`
}

type OpDropColumn struct {
	// Column corresponds to the JSON schema field "column".
	Column string `json:"column" yaml:"column" mapstructure:"column"`

	// Down corresponds to the JSON schema field "down".
	Down *string `json:"down,omitempty" yaml:"down,omitempty" mapstructure:"down,omitempty"`

	// Table corresponds to the JSON schema field "table".
	Table string `json:"table" yaml:"table" mapstructure:"table"`
}

type OpDropConstraint struct {
	// Column corresponds to the JSON schema field "column".
	Column string `json:"column" yaml:"column" mapstructure:"column"`

	// Down corresponds to the JSON schema field "down".
	Down string `json:"down" yaml:"down" mapstructure:"down"`

	// Name corresponds to the JSON schema field "name".
	Name string `json:"name" yaml:"name" mapstructure:"name"`

	// Table corresponds to the JSON schema field "table".
	Table string `json:"table" yaml:"table" mapstructure:"table"`

	// Up corresponds to the JSON schema field "up".
	Up string `json:"up" yaml:"up" mapstructure:"up"`
}

type OpDropIndex struct {
	// Name corresponds to the JSON schema field "name".
	Name string `json:"name" yaml:"name" mapstructure:"name"`
}

type OpDropTable struct {
	// Name corresponds to the JSON schema field "name".
	Name string `json:"name" yaml:"name" mapstructure:"name"`
}

type OpRawSQL struct {
	// Down corresponds to the JSON schema field "down".
	Down string `json:"down,omitempty" yaml:"down,omitempty" mapstructure:"down,omitempty"`

	// Up corresponds to the JSON schema field "up".
	Up string `json:"up" yaml:"up" mapstructure:"up"`
}

type OpRenameTable struct {
	// From corresponds to the JSON schema field "from".
	From string `json:"from" yaml:"from" mapstructure:"from"`

	// To corresponds to the JSON schema field "to".
	To string `json:"to" yaml:"to" mapstructure:"to"`
}

type OpSetReplicaIdentity struct {
	// Identity corresponds to the JSON schema field "identity".
	Identity ReplicaIdentity `json:"identity" yaml:"identity" mapstructure:"identity"`

	// Table corresponds to the JSON schema field "table".
	Table string `json:"table" yaml:"table" mapstructure:"table"`
}

type PgRollMigration struct {
	// Name corresponds to the JSON schema field "name".
	Name string `json:"name" yaml:"name" mapstructure:"name"`

	// Operations corresponds to the JSON schema field "operations".
	Operations []interface{} `json:"operations" yaml:"operations" mapstructure:"operations"`
}

type ReplicaIdentity struct {
	// Index corresponds to the JSON schema field "Index".
	Index string `json:"Index" yaml:"Index" mapstructure:"Index"`

	// Type corresponds to the JSON schema field "Type".
	Type string `json:"Type" yaml:"Type" mapstructure:"Type"`
}

type UniqueConstraint struct {
	// Name corresponds to the JSON schema field "name".
	Name string `json:"name" yaml:"name" mapstructure:"name"`
}
