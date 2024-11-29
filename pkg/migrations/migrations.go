// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	_ "github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

type CallbackFn func(done int64, total int64)

// Operation is an operation that can be applied to a schema
type Operation interface {
	// Start will apply the required changes to enable supporting the new schema
	// version in the database (through a view)
	// update the given views to expose the new schema version
	// Returns the table that requires backfilling, if any.
	Start(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error)

	// Complete will update the database schema to match the current version
	// after calling Start.
	// This method should be called once the previous version is no longer used.
	Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error

	// Rollback will revert the changes made by Start. It is not possible to
	// rollback a completed migration.
	Rollback(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error

	// Validate returns a descriptive error if the operation cannot be applied to the given schema.
	Validate(ctx context.Context, s *schema.Schema) error
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

// SQLTransformer is an interface that can be used to transform SQL statements.
type SQLTransformer interface {
	// TransformSQL will transform the given SQL statement.
	TransformSQL(sql string) (string, error)
}

type SQLTransformerFunc func(string) (string, error)

func (fn SQLTransformerFunc) TransformSQL(sql string) (string, error) {
	return fn(sql)
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

// UpdateVirtualSchema updates the in-memory schema representation with the changes
// made by the migration. No changes are made to the physical database.
func (m *Migration) UpdateVirtualSchema(ctx context.Context, s *schema.Schema) {
	db := &db.FakeDB{}
	tr := SQLTransformerFunc(func(sql string) (string, error) { return sql, nil })

	// Run `Start` on each operation using the fake DB. Updates will be made to
	// the in-memory schema `s` without touching the physical database.
	for _, op := range m.Operations {
		op.Start(ctx, db, "", tr, s)
	}
}

// ContainsRawSQLOperation returns true if the migration contains a raw SQL operation
func (m *Migration) ContainsRawSQLOperation() bool {
	for _, op := range m.Operations {
		if _, ok := op.(*OpRawSQL); ok {
			return true
		}
	}
	return false
}
