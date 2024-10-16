// SPDX-License-Identifier: Apache-2.0
// Code generated by github.com/atombender/go-jsonschema, DO NOT EDIT.

package migrations

import "github.com/oapi-codegen/nullable"

// Check constraint definition
type CheckConstraint struct {
	// Constraint expression
	Constraint string `json:"constraint"`

	// Name of check constraint
	Name string `json:"name"`
}

// Column definition
type Column struct {
	// Check constraint for the column
	Check *CheckConstraint `json:"check,omitempty"`

	// Postgres comment for the column
	Comment *string `json:"comment,omitempty"`

	// Default value for the column
	Default *string `json:"default,omitempty"`

	// Name of the column
	Name string `json:"name"`

	// Indicates if the column is nullable
	Nullable *bool `json:"nullable,omitempty"`

	// Indicates if the column is part of the primary key
	Pk *bool `json:"pk,omitempty"`

	// Foreign key constraint for the column
	References *ForeignKeyReference `json:"references,omitempty"`

	// Postgres type of the column
	Type string `json:"type"`

	// Indicates if the column values must be unique
	Unique *bool `json:"unique,omitempty"`
}

// Foreign key reference definition
type ForeignKeyReference struct {
	// Name of the referenced column
	Column *string `json:"column,omitempty"`

	// Name of the foreign key constraint
	Name string `json:"name"`

	// On delete behavior of the foreign key constraint
	OnDelete ForeignKeyReferenceOnDelete `json:"on_delete,omitempty"`

	// Name of the referenced table
	Table string `json:"table"`
}

type ForeignKeyReferenceOnDelete string

const ForeignKeyReferenceOnDeleteCASCADE ForeignKeyReferenceOnDelete = "CASCADE"
const ForeignKeyReferenceOnDeleteNOACTION ForeignKeyReferenceOnDelete = "NO ACTION"
const ForeignKeyReferenceOnDeleteRESTRICT ForeignKeyReferenceOnDelete = "RESTRICT"
const ForeignKeyReferenceOnDeleteSETDEFAULT ForeignKeyReferenceOnDelete = "SET DEFAULT"
const ForeignKeyReferenceOnDeleteSETNULL ForeignKeyReferenceOnDelete = "SET NULL"

// Add column operation
type OpAddColumn struct {
	// Column to add
	Column Column `json:"column"`

	// Name of the table
	Table string `json:"table"`

	// SQL expression for up migration
	Up string `json:"up,omitempty"`
}

// Alter column operation
type OpAlterColumn struct {
	// Add check constraint to the column
	Check *CheckConstraint `json:"check,omitempty"`

	// Name of the column
	Column string `json:"column"`

	// New comment on the column
	Comment nullable.Nullable[string] `json:"comment,omitempty"`

	// Default value of the column
	Default *string `json:"default,omitempty"`

	// SQL expression for down migration
	Down string `json:"down,omitempty"`

	// New name of the column (for rename column operation)
	Name *string `json:"name,omitempty"`

	// Indicates if the column is nullable (for add/remove not null constraint
	// operation)
	Nullable *bool `json:"nullable,omitempty"`

	// Add foreign key constraint to the column
	References *ForeignKeyReference `json:"references,omitempty"`

	// Name of the table
	Table string `json:"table"`

	// New type of the column (for change type operation)
	Type *string `json:"type,omitempty"`

	// Add unique constraint to the column
	Unique *UniqueConstraint `json:"unique,omitempty"`

	// SQL expression for up migration
	Up string `json:"up,omitempty"`
}

// Create index operation
type OpCreateIndex struct {
	// Names of columns on which to define the index
	Columns []string `json:"columns"`

	// Index name
	Name string `json:"name"`

	// Conditional expression for defining a partial index
	Predicate *string `json:"predicate,omitempty"`

	// Name of table on which to define the index
	Table string `json:"table"`
}

// Create table operation
type OpCreateTable struct {
	// Columns corresponds to the JSON schema field "columns".
	Columns []Column `json:"columns"`

	// Postgres comment for the table
	Comment *string `json:"comment,omitempty"`

	// Name of the table
	Name string `json:"name"`
}

// Drop column operation
type OpDropColumn struct {
	// Name of the column
	Column string `json:"column"`

	// SQL expression for down migration
	Down string `json:"down,omitempty"`

	// Name of the table
	Table string `json:"table"`
}

// Drop constraint operation
type OpDropConstraint struct {
	// Name of the column
	Column string `json:"column"`

	// SQL expression for down migration
	Down string `json:"down"`

	// Name of the constraint
	Name string `json:"name"`

	// Name of the table
	Table string `json:"table"`

	// SQL expression for up migration
	Up string `json:"up"`
}

// Drop index operation
type OpDropIndex struct {
	// Index name
	Name string `json:"name"`
}

// Drop table operation
type OpDropTable struct {
	// Name of the table
	Name string `json:"name"`
}

// Raw SQL operation
type OpRawSQL struct {
	// SQL expression for down migration
	Down string `json:"down,omitempty"`

	// SQL expression will run on complete step (rather than on start)
	OnComplete bool `json:"onComplete,omitempty"`

	// SQL expression for up migration
	Up string `json:"up"`
}

// Rename constraint operation
type OpRenameConstraint struct {
	// Name of the constraint
	From string `json:"from"`

	// Name of the table
	Table string `json:"table"`

	// New name of the constraint
	To string `json:"to"`
}

// Rename table operation
type OpRenameTable struct {
	// Old name of the table
	From string `json:"from"`

	// New name of the table
	To string `json:"to"`
}

// Set replica identity operation
type OpSetReplicaIdentity struct {
	// Replica identity to set
	Identity ReplicaIdentity `json:"identity"`

	// Name of the table
	Table string `json:"table"`
}

// PgRoll migration definition
type PgRollMigration struct {
	// Name of the migration
	Name *string `json:"name,omitempty"`

	// Operations corresponds to the JSON schema field "operations".
	Operations PgRollOperations `json:"operations"`
}

type PgRollOperation interface{}

type PgRollOperations []interface{}

// Replica identity definition
type ReplicaIdentity struct {
	// Name of the index to use as replica identity
	Index string `json:"index"`

	// Type of replica identity
	Type string `json:"type"`
}

// Unique constraint definition
type UniqueConstraint struct {
	// Name of unique constraint
	Name string `json:"name"`
}
