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

	// Generated column definition
	Generated *ColumnGenerated `json:"generated,omitempty"`

	// Name of the column
	Name string `json:"name"`

	// Indicates if the column is nullable
	Nullable bool `json:"nullable,omitempty"`

	// Indicates if the column is part of the primary key
	Pk bool `json:"pk,omitempty"`

	// Foreign key constraint for the column
	References *ForeignKeyReference `json:"references,omitempty"`

	// Postgres type of the column
	Type string `json:"type"`

	// Indicates if the column values must be unique
	Unique bool `json:"unique,omitempty"`
}

// Generated column definition
type ColumnGenerated struct {
	// Expression corresponds to the JSON schema field "expression".
	Expression string `json:"expression,omitempty"`

	// Identity corresponds to the JSON schema field "identity".
	Identity *ColumnGeneratedIdentity `json:"identity,omitempty"`
}

type ColumnGeneratedIdentity struct {
	// SequenceOptions corresponds to the JSON schema field "sequence_options".
	SequenceOptions string `json:"sequence_options,omitempty"`

	// UserSpecifiedValues corresponds to the JSON schema field
	// "user_specified_values".
	UserSpecifiedValues ColumnGeneratedIdentityUserSpecifiedValues `json:"user_specified_values,omitempty"`
}

type ColumnGeneratedIdentityUserSpecifiedValues string

const ColumnGeneratedIdentityUserSpecifiedValuesALWAYS ColumnGeneratedIdentityUserSpecifiedValues = "ALWAYS"
const ColumnGeneratedIdentityUserSpecifiedValuesBYDEFAULT ColumnGeneratedIdentityUserSpecifiedValues = "BY DEFAULT"

// Constraint definition
type Constraint struct {
	// Check constraint expression
	Check string `json:"check,omitempty"`

	// Columns to add constraint to
	Columns []string `json:"columns,omitempty"`

	// Deferable constraint
	Deferrable bool `json:"deferrable,omitempty"`

	// IndexParameters corresponds to the JSON schema field "index_parameters".
	IndexParameters *ConstraintIndexParameters `json:"index_parameters,omitempty"`

	// Initially deferred constraint
	InitiallyDeferred bool `json:"initially_deferred,omitempty"`

	// Name of the constraint
	Name string `json:"name"`

	// Do not propagate constraint to child tables
	NoInherit bool `json:"no_inherit,omitempty"`

	// Nulls not distinct constraint
	NullsNotDistinct bool `json:"nulls_not_distinct,omitempty"`

	// Type of the constraint
	Type ConstraintType `json:"type"`
}

type ConstraintIndexParameters struct {
	// IncludeColumns corresponds to the JSON schema field "include_columns".
	IncludeColumns []string `json:"include_columns,omitempty"`

	// StorageParameters corresponds to the JSON schema field "storage_parameters".
	StorageParameters string `json:"storage_parameters,omitempty"`

	// Tablespace corresponds to the JSON schema field "tablespace".
	Tablespace string `json:"tablespace,omitempty"`
}

type ConstraintType string

const ConstraintTypeCheck ConstraintType = "check"
const ConstraintTypePrimaryKey ConstraintType = "primary_key"
const ConstraintTypeUnique ConstraintType = "unique"

// Foreign key reference definition
type ForeignKeyReference struct {
	// Name of the referenced column
	Column string `json:"column"`

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

// Map of column names to down SQL expressions
type MultiColumnDownSQL map[string]string

// Map of column names to up SQL expressions
type MultiColumnUpSQL map[string]string

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

	// Default value of the column. Setting to null will drop the default if it was
	// set previously.
	Default nullable.Nullable[string] `json:"default,omitempty"`

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

// Add constraint to table operation
type OpCreateConstraint struct {
	// Check constraint expression
	Check *string `json:"check,omitempty"`

	// Columns to add constraint to
	Columns []string `json:"columns,omitempty"`

	// SQL expressions for down migrations
	Down MultiColumnDownSQL `json:"down"`

	// Name of the constraint
	Name string `json:"name"`

	// Reference to the foreign key
	References *OpCreateConstraintReferences `json:"references,omitempty"`

	// Name of the table
	Table string `json:"table"`

	// Type of the constraint
	Type OpCreateConstraintType `json:"type"`

	// SQL expressions for up migrations
	Up MultiColumnUpSQL `json:"up"`
}

// Reference to the foreign key
type OpCreateConstraintReferences struct {
	// Columns to reference
	Columns []string `json:"columns"`

	// On delete behavior of the foreign key constraint
	OnDelete ForeignKeyReferenceOnDelete `json:"on_delete,omitempty"`

	// Name of the table
	Table string `json:"table"`
}

type OpCreateConstraintType string

const OpCreateConstraintTypeCheck OpCreateConstraintType = "check"
const OpCreateConstraintTypeForeignKey OpCreateConstraintType = "foreign_key"
const OpCreateConstraintTypeUnique OpCreateConstraintType = "unique"

// Create index operation
type OpCreateIndex struct {
	// Names of columns on which to define the index
	Columns []string `json:"columns"`

	// Index method to use for the index: btree, hash, gist, spgist, gin, brin
	Method OpCreateIndexMethod `json:"method,omitempty"`

	// Index name
	Name string `json:"name"`

	// Conditional expression for defining a partial index
	Predicate string `json:"predicate,omitempty"`

	// Storage parameters for the index
	StorageParameters string `json:"storage_parameters,omitempty"`

	// Name of table on which to define the index
	Table string `json:"table"`

	// Indicates if the index is unique
	Unique bool `json:"unique,omitempty"`
}

type OpCreateIndexMethod string

const OpCreateIndexMethodBrin OpCreateIndexMethod = "brin"
const OpCreateIndexMethodBtree OpCreateIndexMethod = "btree"
const OpCreateIndexMethodGin OpCreateIndexMethod = "gin"
const OpCreateIndexMethodGist OpCreateIndexMethod = "gist"
const OpCreateIndexMethodHash OpCreateIndexMethod = "hash"
const OpCreateIndexMethodSpgist OpCreateIndexMethod = "spgist"

// Create table operation
type OpCreateTable struct {
	// Columns corresponds to the JSON schema field "columns".
	Columns []Column `json:"columns"`

	// Postgres comment for the table
	Comment *string `json:"comment,omitempty"`

	// Constraints corresponds to the JSON schema field "constraints".
	Constraints []Constraint `json:"constraints,omitempty"`

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

// Drop multi-column constraint operation
type OpDropMultiColumnConstraint struct {
	// SQL expressions for down migrations
	Down MultiColumnDownSQL `json:"down"`

	// Name of the constraint
	Name string `json:"name"`

	// Name of the table
	Table string `json:"table"`

	// SQL expressions for up migrations
	Up MultiColumnUpSQL `json:"up,omitempty"`
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

// Rename column operation
type OpRenameColumn struct {
	// Old name of the column
	From string `json:"from"`

	// Name of the table
	Table string `json:"table"`

	// New name of the column
	To string `json:"to"`
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
