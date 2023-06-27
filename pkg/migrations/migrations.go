package migrations

import (
	"context"
	"database/sql"

	"pg-roll/pkg/schema"

	_ "github.com/lib/pq"
)

type Operation interface {
	// Start will apply the required changes to enable supporting the new schema
	// version in the database (through a view)
	// update the given views to expose the new schema version
	Start(ctx context.Context, conn *sql.DB, v *schema.Schema) error

	// Complete will update the database schema to match the current version
	// after calling Execute.
	// this method should be called once the previous version is no longer used
	Complete(ctx context.Context, conn *sql.DB) error

	// Rollback will revert the changes made by Start. It is not possible to
	// rollback a completed migration.
	Rollback(ctx context.Context, conn *sql.DB) error
}

type (
	Operations []Operation
	Migration  struct {
		Name string `json:"name"`

		Operations Operations `json:"operations"`
	}
)
