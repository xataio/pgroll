// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgroll/pkg/migrations"
)

func TestOpCreateIndexColumns_SetAndGet(t *testing.T) {
	t.Parallel()

	cols := migrations.NewOpCreateIndexColumns()

	// Set multiple columns
	cols.Set("col1", migrations.IndexField{Collate: "en_us"})
	cols.Set("col2", migrations.IndexField{Sort: migrations.IndexFieldSortDESC})

	// Verify Get returns correct values
	settings1, ok1 := cols.Get("col1")
	require.True(t, ok1)
	require.Equal(t, "en_us", settings1.Collate)

	settings2, ok2 := cols.Get("col2")
	require.True(t, ok2)
	require.Equal(t, migrations.IndexFieldSortDESC, settings2.Sort)

	// Verify non-existent column
	_, ok3 := cols.Get("col3")
	require.False(t, ok3)

	// Set duplicate name, verify order preserved (first position)
	cols.Set("col1", migrations.IndexField{Collate: "fr_fr"})
	settings1Updated, _ := cols.Get("col1")
	require.Equal(t, "fr_fr", settings1Updated.Collate)
	require.Equal(t, []string{"col1", "col2"}, cols.Names(), "order should be preserved")
}

func TestOpCreateIndexColumns_OrderPreservation(t *testing.T) {
	t.Parallel()

	// Test 1: Set columns in specific order
	cols1 := migrations.NewOpCreateIndexColumns()
	cols1.Set("col1", migrations.IndexField{})
	cols1.Set("col2", migrations.IndexField{})
	cols1.Set("col3", migrations.IndexField{})

	require.Equal(t, []string{"col1", "col2", "col3"}, cols1.Names())

	// Test 2: Set same columns in different order
	cols2 := migrations.NewOpCreateIndexColumns()
	cols2.Set("col3", migrations.IndexField{})
	cols2.Set("col1", migrations.IndexField{})
	cols2.Set("col2", migrations.IndexField{})

	require.Equal(t, []string{"col3", "col1", "col2"}, cols2.Names())
	require.NotEqual(t, cols1.Names(), cols2.Names())

	// Test 3: OrderedItems returns in correct order
	items := cols1.OrderedItems()
	require.Len(t, items, 3)
	require.Equal(t, "col1", items[0].Name)
	require.Equal(t, "col2", items[1].Name)
	require.Equal(t, "col3", items[2].Name)
}

func TestOpCreateIndexColumns_MarshalJSON(t *testing.T) {
	t.Parallel()

	cols := migrations.NewOpCreateIndexColumns()
	cols.Set("col1", migrations.IndexField{Collate: "en_us"})
	cols.Set("col2", migrations.IndexField{Sort: migrations.IndexFieldSortDESC})

	data, err := json.Marshal(cols)
	require.NoError(t, err)

	// Verify it's a map format
	var m map[string]migrations.IndexField
	err = json.Unmarshal(data, &m)
	require.NoError(t, err)
	require.Len(t, m, 2)
	require.Equal(t, "en_us", m["col1"].Collate)
	require.Equal(t, migrations.IndexFieldSortDESC, m["col2"].Sort)

	// Verify order is preserved in JSON (Go 1.12+ preserves map key order)
	// We can't directly test this, but we can verify the unmarshal preserves it
	var cols2 migrations.OpCreateIndexColumns
	err = json.Unmarshal(data, &cols2)
	require.NoError(t, err)
	require.Equal(t, []string{"col1", "col2"}, cols2.Names())
}

func TestOpCreateIndexColumns_UnmarshalJSON_MapFormat(t *testing.T) {
	t.Parallel()

	// Test map format (current format) - order should be preserved from JSON
	jsonData := []byte(`{"col1": {}, "col2": {"collate": "en_us"}, "col3": {"sort": "DESC"}}`)
	var cols migrations.OpCreateIndexColumns
	err := json.Unmarshal(jsonData, &cols)
	require.NoError(t, err)

	// Verify order is preserved from JSON
	require.Equal(t, []string{"col1", "col2", "col3"}, cols.Names())

	// Verify settings are preserved
	settings1, _ := cols.Get("col1")
	require.Equal(t, migrations.IndexField{}, settings1)

	settings2, _ := cols.Get("col2")
	require.Equal(t, "en_us", settings2.Collate)

	settings3, _ := cols.Get("col3")
	require.Equal(t, migrations.IndexFieldSortDESC, settings3.Sort)

	// Test that order from JSON is preserved (not alphabetical)
	jsonData2 := []byte(`{"z_col": {}, "a_col": {}, "m_col": {}}`)
	var cols2 migrations.OpCreateIndexColumns
	err = json.Unmarshal(jsonData2, &cols2)
	require.NoError(t, err)
	
	// Order should be z, a, m (as in JSON), NOT a, m, z (alphabetical)
	require.Equal(t, []string{"z_col", "a_col", "m_col"}, cols2.Names(), "order should match JSON, not be alphabetical")
}

func TestOpCreateIndexColumns_EdgeCases(t *testing.T) {
	t.Parallel()

	// Test empty columns
	cols1 := migrations.NewOpCreateIndexColumns()
	require.Equal(t, 0, cols1.Len())
	require.Equal(t, []string{}, cols1.Names())
	require.Len(t, cols1.OrderedItems(), 0)

	data, err := json.Marshal(cols1)
	require.NoError(t, err)
	require.Equal(t, "{}", string(data))

	// Test single column
	cols2 := migrations.NewOpCreateIndexColumns()
	cols2.Set("col1", migrations.IndexField{})
	require.Equal(t, 1, cols2.Len())
	require.Equal(t, []string{"col1"}, cols2.Names())

	// Test many columns (10+)
	cols3 := migrations.NewOpCreateIndexColumns()
	for i := 0; i < 15; i++ {
		cols3.Set(fmt.Sprintf("col%d", i), migrations.IndexField{})
	}
	require.Equal(t, 15, cols3.Len())
	require.Len(t, cols3.Names(), 15)
}

func TestOpCreateIndexColumns_RoundTrip(t *testing.T) {
	t.Parallel()

	// Test that marshal/unmarshal preserves data and order
	cols1 := migrations.NewOpCreateIndexColumns()
	cols1.Set("col1", migrations.IndexField{Collate: "en_us"})
	cols1.Set("col2", migrations.IndexField{Sort: migrations.IndexFieldSortDESC})

	data, err := json.Marshal(cols1)
	require.NoError(t, err)

	var cols2 migrations.OpCreateIndexColumns
	err = json.Unmarshal(data, &cols2)
	require.NoError(t, err)
	require.Equal(t, cols1.Names(), cols2.Names())

	settings1, _ := cols2.Get("col1")
	require.Equal(t, "en_us", settings1.Collate)

	settings2, _ := cols2.Get("col2")
	require.Equal(t, migrations.IndexFieldSortDESC, settings2.Sort)
}

func TestOpCreateIndexColumns_MarshalPreservesOrder(t *testing.T) {
	t.Parallel()

	// Test that marshaling preserves the insertion order in the JSON string
	// This is critical for consistent output when converting SQL to migrations
	cols := migrations.NewOpCreateIndexColumns()
	cols.Set("zebra", migrations.IndexField{})
	cols.Set("alpha", migrations.IndexField{})
	cols.Set("beta", migrations.IndexField{})

	data, err := json.Marshal(cols)
	require.NoError(t, err)

	// The JSON should have keys in insertion order (zebra, alpha, beta)
	// not alphabetical order (alpha, beta, zebra)
	jsonStr := string(data)
	
	// Find positions of each key in the JSON string
	zebraPos := -1
	alphaPos := -1
	betaPos := -1
	
	for i := 0; i < len(jsonStr)-6; i++ {
		if jsonStr[i:i+7] == `"zebra"` {
			zebraPos = i
		}
		if jsonStr[i:i+7] == `"alpha"` {
			alphaPos = i
		}
		if jsonStr[i:i+6] == `"beta"` {
			betaPos = i
		}
	}
	
	require.NotEqual(t, -1, zebraPos, "zebra should be in JSON")
	require.NotEqual(t, -1, alphaPos, "alpha should be in JSON")
	require.NotEqual(t, -1, betaPos, "beta should be in JSON")
	
	// Verify insertion order is preserved
	require.Less(t, zebraPos, alphaPos, "zebra should appear before alpha")
	require.Less(t, alphaPos, betaPos, "alpha should appear before beta")
}

func TestOpCreateIndexColumns_JSONToOperationOrder(t *testing.T) {
	t.Parallel()

	// Test the primary use case: reading a migration JSON file
	// with specific column order for index creation
	jsonInput := `{
		"name": "idx_users",
		"table": "users",
		"columns": {
			"last_name": {},
			"first_name": {},
			"email": {}
		},
		"method": "btree"
	}`

	var op migrations.OpCreateIndex
	err := json.Unmarshal([]byte(jsonInput), &op)
	require.NoError(t, err)

	// Verify column order from JSON is preserved
	expectedOrder := []string{"last_name", "first_name", "email"}
	require.Equal(t, expectedOrder, op.Columns.Names(),
		"Column order from JSON must be preserved for correct index creation")

	// Verify OrderedItems returns columns in correct order for SQL generation
	orderedItems := op.Columns.OrderedItems()
	require.Len(t, orderedItems, 3)
	require.Equal(t, "last_name", orderedItems[0].Name)
	require.Equal(t, "first_name", orderedItems[1].Name)
	require.Equal(t, "email", orderedItems[2].Name)
}
