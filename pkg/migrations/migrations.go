package migrations

import (
	"context"
	"database/sql"

	_ "github.com/lib/pq"
	"github.com/xataio/pg-roll/pkg/schema"
)

type Operation interface {
	// Start will apply the required changes to enable supporting the new schema
	// version in the database (through a view)
	// update the given views to expose the new schema version
	Start(ctx context.Context, conn *sql.DB, stateSchema string, s *schema.Schema) error

	// Complete will update the database schema to match the current version
	// after calling Start.
	// This method should be called once the previous version is no longer used
	Complete(ctx context.Context, conn *sql.DB) error

	// Rollback will revert the changes made by Start. It is not possible to
	// rollback a completed migration.
	Rollback(ctx context.Context, conn *sql.DB) error

	// Validate returns a descriptive error if the operation cannot be applied to the given schema
	Validate(ctx context.Context, s *schema.Schema) error
}

type (
	Operations []Operation
	Migration  struct {
		Name string `json:"name"`

		Operations Operations `json:"operations"`
	}
)

// Validate will check that the migration can be applied to the given schema
// returns a descriptive error if the migration is invalid
func (m *Migration) Validate(ctx context.Context, s *schema.Schema) error {
	for _, op := range m.Operations {
		err := op.Validate(ctx, s)
		if err != nil {
			return err
		}
	}

	return nil
}
