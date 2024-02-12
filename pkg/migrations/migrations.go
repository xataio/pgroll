// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/schema"
)

type CallbackFn func(int64)

type Operation interface {
	// Start will apply the required changes to enable supporting the new schema
	// version in the database (through a view)
	// update the given views to expose the new schema version
	Start(ctx context.Context, conn *sql.DB, stateSchema string, s *schema.Schema, cbs ...CallbackFn) error

	// Complete will update the database schema to match the current version
	// after calling Start.
	// This method should be called once the previous version is no longer used
	Complete(ctx context.Context, conn *sql.DB, s *schema.Schema) error

	// Rollback will revert the changes made by Start. It is not possible to
	// rollback a completed migration.
	Rollback(ctx context.Context, conn *sql.DB) error

	// Validate returns a descriptive error if the operation cannot be applied to the given schema
	Validate(ctx context.Context, s *schema.Schema) error
}

// IsolatedOperation is an operation that cannot be executed with other operations
// in the same migration
type IsolatedOperation interface {
	// this operation is isolated when executed on start, cannot be executed with other operations
	IsIsolated() bool
}

// RequiresSchemaRefreshOperation is an operation that requires the resulting schema to be refreshed
type RequiresSchemaRefreshOperation interface {
	// this operation requires the resulting schema to be refreshed when executed on start
	RequiresSchemaRefresh()
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
		if isolatedOp, ok := op.(IsolatedOperation); ok {
			if isolatedOp.IsIsolated() && len(m.Operations) > 1 {
				return InvalidMigrationError{Reason: fmt.Sprintf("operation %q cannot be executed with other operations", OperationName(op))}
			}
		}
	}

	for _, op := range m.Operations {
		err := op.Validate(ctx, s)
		if err != nil {
			return err
		}
	}

	return nil
}
