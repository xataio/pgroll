// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/migrations"
)

func TestFileUpdater(t *testing.T) {
	updater := migrations.NewFileUpdater()

	t.Run("update with no operations", func(t *testing.T) {
		rawMigration := &migrations.RawMigration{Operations: []byte("[]")}
		migration, err := updater.Update(rawMigration)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(migration.Operations) != 0 {
			t.Fatalf("expected no operations, got %d", len(migration.Operations))
		}
	})

	t.Run("update with malformed operations", func(t *testing.T) {
		rawMigration := &migrations.RawMigration{
			Operations: []byte(`[{"create_index": {"name": "idx_test", "columns": "col1, col2"}}]`),
		}
		_, err := updater.Update(rawMigration)
		if err == nil {
			t.Fatal("expected error for malformed operations, got nil")
		}
	})

	t.Run("update with create_index operation", func(t *testing.T) {
		rawMigration := &migrations.RawMigration{
			Operations: []byte(`[{"create_index": {"name": "idx_test", "columns": ["col1", "col2"]}}]`),
		}
		migration, err := updater.Update(rawMigration)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(migration.Operations) != 1 {
			t.Fatalf("expected 1 operation, got %d", len(migration.Operations))
		}
		op := migration.Operations[0]
		createIndexOp, ok := op.(*migrations.OpCreateIndex)
		if !ok {
			t.Fatal("expected create_index operation to be present")
		}

		if createIndexOp.Columns == nil {
			t.Fatal("expected columns to be a map, got nil")
		}

		var expectedColumns = migrations.OpCreateIndexColumns{
			"col1": migrations.IndexField{},
			"col2": migrations.IndexField{},
		}
		assert.Equal(t, expectedColumns, createIndexOp.Columns, "columns should be transformed to a map")
	})

	t.Run("update with no create_index operation", func(t *testing.T) {
		rawMigration := &migrations.RawMigration{
			Operations: []byte(`[{"create_table": {"name": "test_table"}}]`),
		}
		migration, err := updater.Update(rawMigration)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(migration.Operations) != 1 {
			t.Fatalf("expected 1 operation, got %d", len(migration.Operations))
		}
		op := migration.Operations[0]
		if _, ok := op.(*migrations.OpCreateTable); !ok {
			t.Fatal("expected create_table operation to be present")
		}
	})

	t.Run("update with multiple operations", func(t *testing.T) {
		rawMigration := &migrations.RawMigration{
			Operations: []byte(`[
				{"create_index": {"name": "idx_test1", "columns": ["col1"]}},
				{"create_index": {"name": "idx_test2", "columns": ["col2"]}}
			]`),
		}
		migration, err := updater.Update(rawMigration)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(migration.Operations) != 2 {
			t.Fatalf("expected 2 operations, got %d", len(migration.Operations))
		}
		for i, op := range migration.Operations {
			createIndexOp, ok := op.(*migrations.OpCreateIndex)
			if !ok {
				t.Fatalf("expected create_index operation at index %d to be present", i)
			}
			if createIndexOp.Columns == nil {
				t.Fatal("expected columns to be a map, got nil")
			}
		}
	})

	t.Run("update with multiple operations and one create_index", func(t *testing.T) {
		rawMigration := &migrations.RawMigration{
			Operations: []byte(`[
				{"create_table": {"name": "test_table"}},
				{"create_index": {"name": "idx_test", "columns": ["col1", "col2"]}}
			]`),
		}
		migration, err := updater.Update(rawMigration)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(migration.Operations) != 2 {
			t.Fatalf("expected 2 operations, got %d", len(migration.Operations))
		}
		op := migration.Operations[0]
		if _, ok := op.(*migrations.OpCreateTable); !ok {
			t.Fatal("expected create_table operation to be present")
		}

		op = migration.Operations[1]
		createIndexOp, ok := op.(*migrations.OpCreateIndex)
		if !ok {
			t.Fatal("expected create_index operation to be present")
		}

		if createIndexOp.Columns == nil {
			t.Fatal("expected columns to be a map, got nil")
		}

		var expectedColumns = migrations.OpCreateIndexColumns{
			"col1": migrations.IndexField{},
			"col2": migrations.IndexField{},
		}
		assert.Equal(t, expectedColumns, createIndexOp.Columns, "columns should be transformed to a map")
	})
}
