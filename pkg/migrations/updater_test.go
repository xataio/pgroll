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

		t.Run("update from old string array format", func(t *testing.T) {
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

			// Order is preserved when converting from string array
			expectedColumns := []migrations.IndexField{
				{Column: "col1"},
				{Column: "col2"},
			}
			require.Equal(t, expectedColumns, createIndexOp.Columns, "columns should be transformed to array format preserving order")
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
				require.Len(t, createIndexOp.Columns, 1, "expected 1 column for create_index operation at index %d", i)
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
			require.NotNil(t, createIndexOp.Columns, "expected columns to be present")

			expectedColumns := []migrations.IndexField{
				{Column: "col1"},
				{Column: "col2"},
			}
			require.Equal(t, expectedColumns, createIndexOp.Columns, "columns should be transformed to array format")
		})

		t.Run("update from old map format", func(t *testing.T) {
			rawMigration := &migrations.RawMigration{
				Operations: []byte(`[{"create_index": {"name": "idx_test", "columns": {"col1": {}, "col2": {"sort": "DESC"}}}}]`),
			}
			migration, err := updater.Update(rawMigration)
			require.NoError(t, err, "expected no error for valid create_index operation")
			require.Equal(t, 1, len(migration.Operations), "expected 1 operation in migration")

			op := migration.Operations[0]
			createIndexOp, ok := op.(*migrations.OpCreateIndex)
			require.True(t, ok, "expected create_index operation to be present")
			require.NotNil(t, createIndexOp.Columns, "expected columns to be present")
			require.Len(t, createIndexOp.Columns, 2, "expected 2 columns")

			// Check that both columns exist with correct settings (order may vary
			// due to map iteration)
			columnSettings := make(map[string]migrations.IndexField)
			for _, col := range createIndexOp.Columns {
				columnSettings[col.Column] = col
			}
			require.Contains(t, columnSettings, "col1", "expected col1 to be present")
			require.Contains(t, columnSettings, "col2", "expected col2 to be present")
			require.Equal(t, migrations.IndexFieldSortDESC, columnSettings["col2"].Sort, "expected col2 to have DESC sort")
		})

		t.Run("passthrough new array format", func(t *testing.T) {
			rawMigration := &migrations.RawMigration{
				Operations: []byte(`[{"create_index": {"name": "idx_test", "columns": [{"column": "col1"}, {"column": "col2"}]}}]`),
			}
			migration, err := updater.Update(rawMigration)
			require.NoError(t, err, "expected no error for valid create_index operation")
			require.Equal(t, 1, len(migration.Operations), "expected 1 operation in migration")

			op := migration.Operations[0]
			createIndexOp, ok := op.(*migrations.OpCreateIndex)
			require.True(t, ok, "expected create_index operation to be present")
			require.NotNil(t, createIndexOp.Columns, "expected columns to be present")

			expectedColumns := []migrations.IndexField{
				{Column: "col1"},
				{Column: "col2"},
			}
			require.Equal(t, expectedColumns, createIndexOp.Columns, "columns should be preserved in order")
		})
	})
}

func createIndexUpdater() *migrations.FileUpdater {
	return migrations.NewFileUpdater(map[string][]migrations.UpdaterFn{
		string(migrations.OpNameCreateIndex): {
			migrations.UpdateCreateIndexColumnsList,       // string[] -> map
			migrations.UpdateCreateIndexColumnsMapToArray, // map -> array[]
		},
	})
}
