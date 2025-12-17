// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgroll/pkg/migrations"
)

func TestFileUpdater(t *testing.T) {
	t.Run("create_index", func(t *testing.T) {
		updater := createIndexUpdater()

		t.Run("update with no operations", func(t *testing.T) {
			rawMigration := &migrations.RawMigration{Operations: []byte("[]")}
			migration, err := updater.Update(rawMigration)
			require.NoError(t, err, "expected no error for empty operations")
			require.Equal(t, 0, len(migration.Operations), "expected no operations in migration")
		})

		t.Run("update with malformed operations", func(t *testing.T) {
			rawMigration := &migrations.RawMigration{
				Operations: []byte(`[{"create_index": {"name": "idx_test", "columns": "col1, col2"}}]`),
			}
			_, err := updater.Update(rawMigration)
			require.Error(t, err, "expected error for malformed operations")
		})

		t.Run("update with create_index operation", func(t *testing.T) {
			rawMigration := &migrations.RawMigration{
				Operations: []byte(`[{"create_index": {"name": "idx_test", "columns": ["col1", "col2"]}}]`),
			}
			migration, err := updater.Update(rawMigration)
			require.NoError(t, err, "expected no error for valid create_index operation")
			require.Equal(t, 1, len(migration.Operations), "expected 1 operation in migration")

			op := migration.Operations[0]
			createIndexOp, ok := op.(*migrations.OpCreateIndex)
			require.True(t, ok, "expected create_index operation to be present")
			require.NotNil(t, createIndexOp.Columns, "expected columns to be present")

			expectedColumns := migrations.NewOpCreateIndexColumns()
			expectedColumns.Set("col1", migrations.IndexField{})
			expectedColumns.Set("col2", migrations.IndexField{})
			require.Equal(t, expectedColumns.Names(), createIndexOp.Columns.Names(), "columns should be transformed and preserve order")
			require.Equal(t, 2, createIndexOp.Columns.Len(), "columns should have 2 items")
		})

		t.Run("update with no create_index operation", func(t *testing.T) {
			rawMigration := &migrations.RawMigration{
				Operations: []byte(`[{"create_table": {"name": "test_table"}}]`),
			}
			migration, err := updater.Update(rawMigration)
			require.NoError(t, err, "expected no error for valid create_table operation")
			require.Equal(t, 1, len(migration.Operations), "expected 1 operation in migration")

			op := migration.Operations[0]
			_, ok := op.(*migrations.OpCreateTable)
			require.True(t, ok, "expected create_table operation to be present")
		})

		t.Run("update with multiple operations", func(t *testing.T) {
			rawMigration := &migrations.RawMigration{
				Operations: []byte(`[
				{"create_index": {"name": "idx_test1", "columns": ["col1"]}},
				{"create_index": {"name": "idx_test2", "columns": ["col2"]}}
			]`),
			}
			migration, err := updater.Update(rawMigration)
			require.NoError(t, err, "expected no error for multiple create_index operations")
			require.Equal(t, 2, len(migration.Operations), "expected 2 operations in migration")

			for i, op := range migration.Operations {
				createIndexOp, ok := op.(*migrations.OpCreateIndex)
				require.True(t, ok, "expected create_index operation at index %d to be present", i)
				require.NotNil(t, createIndexOp.Columns, "expected columns to be present for create_index operation at index %d", i)
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
			require.NoError(t, err, "expected no error for mixed operations")
			require.Equal(t, 2, len(migration.Operations), "expected 2 operations in migration")

			op := migration.Operations[0]
			_, ok := op.(*migrations.OpCreateTable)
			require.True(t, ok, "expected create_table operation to be present")

			op = migration.Operations[1]
			createIndexOp, ok := op.(*migrations.OpCreateIndex)
			require.True(t, ok, "expected create_index operation to be present")
			require.NotNil(t, createIndexOp.Columns, "expected columns to be present for create_index operation")

			expectedColumns := migrations.NewOpCreateIndexColumns()
			expectedColumns.Set("col1", migrations.IndexField{})
			expectedColumns.Set("col2", migrations.IndexField{})
			require.Equal(t, expectedColumns.Names(), createIndexOp.Columns.Names(), "columns should be transformed and preserve order")
			require.Equal(t, 2, createIndexOp.Columns.Len(), "columns should have 2 items")
		})
	})
}

func createIndexUpdater() *migrations.FileUpdater {
	return migrations.NewFileUpdater(map[string][]migrations.UpdaterFn{
		string(migrations.OpNameCreateIndex): {
			migrations.UpdateCreateIndexColumnsList,
		},
	})
}
