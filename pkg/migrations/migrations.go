// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"encoding/json"
	"fmt"

	_ "github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

// Operation is an operation that can be applied to a schema
type Operation interface {
	// Start will return the list of required changes to enable supporting the new schema
	// version in the database (through a view)
	// update the given views to expose the new schema version
	// Returns the table that requires backfilling, if any.
	Start(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) (*StartResult, error)

	// Complete will update the database schema to match the current version
	// after calling Start.
	// This method should be called once the previous version is no longer used.
	Complete(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error)

	// Rollback will revert the changes made by Start. It is not possible to
	// rollback a completed migration.
	Rollback(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error)

	// Validate returns a descriptive error if the operation cannot be applied to the given schema.
	Validate(ctx context.Context, s *schema.Schema) error
}

// Createable interface must be implemented for all operations
// that can be created using the CLI create command.
//
// The function must prompt users to configure all attributes of an operation.
//
// Example implementation for OpMyOperation that has 3 attributes: table, column and down:
//
//	func (o *OpMyOperation) Create() {
//		o.Table, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("table").Show()
//		o.Column, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("column").Show()
//		o.Down, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("down").Show()
//	}
type Createable interface {
	Create()
}

// IsolatedOperation is an operation that cannot be executed with other operations
// in the same migration.
type IsolatedOperation interface {
	// IsIsolated defines where this operation is isolated when executed on start, cannot be executed
	// with other operations.
	IsIsolated() bool
}

// RequiresSchemaRefreshOperation is an operation that requires the resulting schema to be refreshed.
type RequiresSchemaRefreshOperation interface {
	// RequiresSchemaRefresh defines if this operation requires the resulting schema to be refreshed when
	// executed on start.
	RequiresSchemaRefresh()
}

type (
	Operations []Operation
	Migration  struct {
		Name          string     `json:"-"`
		VersionSchema string     `json:"version_schema,omitempty"`
		Operations    Operations `json:"operations"`
	}
	RawMigration struct {
		Name          string          `json:"-"`
		VersionSchema string          `json:"version_schema,omitempty"`
		Operations    json.RawMessage `json:"operations"`
	}

	StartResult struct {
		Actions      []DBAction
		BackfillTask *backfill.Task
	}
)

// VersionSchemaName returns the version schema name for the migration.
// It defaults to the migration name if no version schema is set.
func (m *Migration) VersionSchemaName() string {
	if m.VersionSchema != "" {
		return m.VersionSchema
	}
	return m.Name
}

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

// UpdateVirtualSchema updates the in-memory schema representation with the changes
// made by the migration. No changes are made to the physical database.
func (m *Migration) UpdateVirtualSchema(ctx context.Context, s *schema.Schema) error {
	db := &db.FakeDB{}

	// Run `Start` on each operation using the fake DB. Updates will be made to
	// the in-memory schema `s` without touching the physical database.
	for _, op := range m.Operations {
		if _, err := op.Start(ctx, NewNoopLogger(), db, s); err != nil {
			return err
		}
	}
	return nil
}
