// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/xataio/pgroll/pkg/migrations"
)

// TestOpCreateIndex_UnmarshalFormats tests direct unmarshaling of both formats
func TestOpCreateIndex_UnmarshalFormats(t *testing.T) {
	t.Parallel()

	t.Run("JSON array format", func(t *testing.T) {
		jsonData := `{
			"name": "idx_test",
			"table": "users",
			"columns": [
				{"name": "col1"},
				{"name": "col2", "sort": "DESC"},
				{"name": "col3", "collate": "en_us"}
			]
		}`

		var op migrations.OpCreateIndex
		err := json.Unmarshal([]byte(jsonData), &op)
		require.NoError(t, err)
		require.Len(t, op.Columns, 3)
		
		// Verify order is preserved
		assert.Equal(t, "col1", op.Columns[0].Name)
		assert.Equal(t, "col2", op.Columns[1].Name)
		assert.Equal(t, migrations.IndexFieldSortDESC, op.Columns[1].Sort)
		assert.Equal(t, "col3", op.Columns[2].Name)
		assert.Equal(t, "en_us", op.Columns[2].Collate)
	})

	t.Run("JSON map format", func(t *testing.T) {
		jsonData := `{
			"name": "idx_test",
			"table": "users",
			"columns": {
				"col1": {},
				"col2": {"sort": "DESC"},
				"col3": {"collate": "en_us"}
			}
		}`

		var op migrations.OpCreateIndex
		err := json.Unmarshal([]byte(jsonData), &op)
		require.NoError(t, err)
		assert.Len(t, op.Columns, 3)
		
		// Verify all columns present (order may vary in JSON maps)
		colNames := make(map[string]bool)
		for _, col := range op.Columns {
			colNames[col.Name] = true
		}
		assert.True(t, colNames["col1"])
		assert.True(t, colNames["col2"])
		assert.True(t, colNames["col3"])
	})

	t.Run("YAML array format", func(t *testing.T) {
		yamlData := `
name: idx_test
table: users
columns:
  - name: col1
  - name: col2
    sort: DESC
  - name: col3
    collate: en_us
`
		var op migrations.OpCreateIndex
		err := yaml.Unmarshal([]byte(yamlData), &op)
		require.NoError(t, err)
		require.Len(t, op.Columns, 3)
		
		// Verify order is preserved
		assert.Equal(t, "col1", op.Columns[0].Name)
		assert.Equal(t, "col2", op.Columns[1].Name)
		assert.Equal(t, "col3", op.Columns[2].Name)
	})

	t.Run("YAML map format preserves key order", func(t *testing.T) {
		yamlData := `
name: idx_test
table: users
columns:
  zebra: {}
  alpha:
    sort: DESC
  beta:
    collate: en_us
`
		var op migrations.OpCreateIndex
		err := yaml.Unmarshal([]byte(yamlData), &op)
		require.NoError(t, err)
		require.Len(t, op.Columns, 3)
		
		// Verify YAML preserves key order (zebra, alpha, beta - not alphabetical)
		assert.Equal(t, "zebra", op.Columns[0].Name)
		assert.Equal(t, "alpha", op.Columns[1].Name)
		assert.Equal(t, "beta", op.Columns[2].Name)
	})
}

// TestBackwardCompatibility verifies existing migrations work unchanged
func TestBackwardCompatibility(t *testing.T) {
	t.Parallel()

	t.Run("single column map format", func(t *testing.T) {
		jsonData := `{
			"name": "create_index",
			"operations": [{
				"create_index": {
					"name": "idx_users_email",
					"table": "users",
					"columns": {"email": {}}
				}
			}]
		}`

		var rawMig migrations.RawMigration
		err := json.Unmarshal([]byte(jsonData), &rawMig)
		require.NoError(t, err)

		mig, err := migrations.ParseMigration(&rawMig)
		require.NoError(t, err)
		require.Len(t, mig.Operations, 1)

		op := mig.Operations[0].(*migrations.OpCreateIndex)
		assert.Equal(t, "idx_users_email", op.Name)
		assert.Equal(t, "email", op.Columns[0].Name)
	})

	t.Run("map format with settings", func(t *testing.T) {
		jsonData := `{
			"name": "create_index",
			"operations": [{
				"create_index": {
					"name": "idx_users_created",
					"table": "users",
					"columns": {
						"created_at": {"sort": "DESC", "nulls": "LAST"}
					}
				}
			}]
		}`

		var rawMig migrations.RawMigration
		err := json.Unmarshal([]byte(jsonData), &rawMig)
		require.NoError(t, err)

		mig, err := migrations.ParseMigration(&rawMig)
		require.NoError(t, err)

		op := mig.Operations[0].(*migrations.OpCreateIndex)
		assert.Equal(t, "created_at", op.Columns[0].Name)
		assert.Equal(t, migrations.IndexFieldSortDESC, op.Columns[0].Sort)
		assert.Equal(t, migrations.IndexFieldNullsLAST, *op.Columns[0].Nulls)
	})
}

// TestArrayFormat_OrderPreservation verifies correct ordering behavior
func TestArrayFormat_OrderPreservation(t *testing.T) {
	t.Parallel()

	t.Run("multi-column array preserves order", func(t *testing.T) {
		jsonData := `{
			"name": "create_index",
			"operations": [{
				"create_index": {
					"name": "idx_users_name_email",
					"table": "users",
					"columns": [
						{"name": "last_name"},
						{"name": "first_name"},
						{"name": "email", "sort": "DESC"}
					]
				}
			}]
		}`

		var rawMig migrations.RawMigration
		err := json.Unmarshal([]byte(jsonData), &rawMig)
		require.NoError(t, err)

		mig, err := migrations.ParseMigration(&rawMig)
		require.NoError(t, err)

		op := mig.Operations[0].(*migrations.OpCreateIndex)
		require.Len(t, op.Columns, 3)
		assert.Equal(t, "last_name", op.Columns[0].Name)
		assert.Equal(t, "first_name", op.Columns[1].Name)
		assert.Equal(t, "email", op.Columns[2].Name)
		assert.Equal(t, migrations.IndexFieldSortDESC, op.Columns[2].Sort)
	})

	t.Run("non-alphabetical order preserved", func(t *testing.T) {
		jsonData := `{
			"name": "create_index",
			"operations": [{
				"create_index": {
					"name": "idx_test",
					"table": "users",
					"columns": [
						{"name": "zebra"},
						{"name": "alpha"},
						{"name": "beta"}
					]
				}
			}]
		}`

		var rawMig migrations.RawMigration
		err := json.Unmarshal([]byte(jsonData), &rawMig)
		require.NoError(t, err)

		mig, err := migrations.ParseMigration(&rawMig)
		require.NoError(t, err)

		op := mig.Operations[0].(*migrations.OpCreateIndex)
		// Verify: zebra, alpha, beta (NOT alphabetical)
		assert.Equal(t, "zebra", op.Columns[0].Name)
		assert.Equal(t, "alpha", op.Columns[1].Name)
		assert.Equal(t, "beta", op.Columns[2].Name)
	})
}

// TestMixedFormatsInMigration verifies both formats can coexist
func TestMixedFormatsInMigration(t *testing.T) {
	t.Parallel()

	jsonData := `{
		"name": "mixed_indexes",
		"operations": [
			{
				"create_index": {
					"name": "idx_single",
					"table": "users",
					"columns": {"email": {}}
				}
			},
			{
				"create_index": {
					"name": "idx_multi",
					"table": "users",
					"columns": [
						{"name": "last_name"},
						{"name": "first_name"}
					]
				}
			}
		]
	}`

	var rawMig migrations.RawMigration
	err := json.Unmarshal([]byte(jsonData), &rawMig)
	require.NoError(t, err)

	mig, err := migrations.ParseMigration(&rawMig)
	require.NoError(t, err)
	require.Len(t, mig.Operations, 2)

	// Map format
	op1 := mig.Operations[0].(*migrations.OpCreateIndex)
	assert.Equal(t, "idx_single", op1.Name)
	assert.Equal(t, "email", op1.Columns[0].Name)

	// Array format
	op2 := mig.Operations[1].(*migrations.OpCreateIndex)
	assert.Equal(t, "idx_multi", op2.Name)
	assert.Equal(t, "last_name", op2.Columns[0].Name)
	assert.Equal(t, "first_name", op2.Columns[1].Name)
}
