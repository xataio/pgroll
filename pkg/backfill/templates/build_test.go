// SPDX-License-Identifier: Apache-2.0

package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateBatchTable(t *testing.T) {
	tests := map[string]struct {
		config   CreateBatchTableConfig
		expected string
	}{
		"in pgroll schema, single identity column": {
			config: CreateBatchTableConfig{
				StateSchema:      "pgroll",
				TableName:        "items",
				BatchTablePrefix: "batch_",
				IDColumns:        []string{"id"},
			},
			expected: createBatchTable1,
		},
		"in other schema, single identity column": {
			config: CreateBatchTableConfig{
				StateSchema:      "other",
				TableName:        "items",
				BatchTablePrefix: "batch_",
				IDColumns:        []string{"id"},
			},
			expected: createBatchTable2,
		},
		"in pgroll schema, multiple identity columns": {
			config: CreateBatchTableConfig{
				StateSchema:      "pgroll",
				TableName:        "items",
				BatchTablePrefix: "batch_",
				IDColumns:        []string{"id", "zip"},
			},
			expected: createBatchTable3,
		},
		"in pgroll schema, products table, multiple identity columns": {
			config: CreateBatchTableConfig{
				StateSchema:      "pgroll",
				TableName:        "products",
				BatchTablePrefix: "batch_",
				IDColumns:        []string{"id", "zip"},
			},
			expected: createBatchTable4,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			actual, err := BuildCreateBatchTable(test.config)
			assert.NoError(t, err)

			assert.Equal(t, test.expected, actual)
		})
	}
}

func TestSelectBatchInto(t *testing.T) {
	tests := map[string]struct {
		config   BatchConfig
		expected string
	}{
		"first batch (no last pk values)": {
			config: BatchConfig{
				TableName:        "items",
				PrimaryKey:       []string{"id"},
				BatchSize:        1000,
				StateSchema:      "pgroll",
				BatchTablePrefix: "batch_",
			},
			expected: selectBatchInto1,
		},
		"batch with last pk values (non-initial batch)": {
			config: BatchConfig{
				TableName:        "items",
				PrimaryKey:       []string{"id"},
				LastValue:        []string{"100"},
				BatchSize:        1000,
				StateSchema:      "pgroll",
				BatchTablePrefix: "batch_",
			},
			expected: selectBatchInto2,
		},
		"batch for a table with multi-column primary key": {
			config: BatchConfig{
				TableName:        "products",
				PrimaryKey:       []string{"id", "zip"},
				LastValue:        []string{"100", "abc"},
				BatchSize:        2000,
				StateSchema:      "pgroll",
				BatchTablePrefix: "batch_",
			},
			expected: selectBatchInto3,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			actual, err := BuildSelectBatchInto(test.config)
			assert.NoError(t, err)

			assert.Equal(t, test.expected, actual)
		})
	}
}

func TestUpdateBatch(t *testing.T) {
	tests := map[string]struct {
		config   BatchConfig
		expected string
	}{
		"single column primary key": {
			config: BatchConfig{
				TableName:        "items",
				PrimaryKey:       []string{"id"},
				StateSchema:      "pgroll",
				BatchTablePrefix: "batch_",
			},
			expected: updateBatch1,
		},
		"multi column primary key": {
			config: BatchConfig{
				TableName:        "products",
				PrimaryKey:       []string{"id", "zip"},
				StateSchema:      "pgroll",
				BatchTablePrefix: "batch_",
			},
			expected: updateBatch2,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			actual, err := BuildUpdateBatch(test.config)
			assert.NoError(t, err)

			assert.Equal(t, test.expected, actual)
		})
	}
}

const createBatchTable1 = `CREATE UNLOGGED TABLE IF NOT EXISTS
  "pgroll"."batch_items" AS
  SELECT "id"
  FROM "items"
  WHERE false
`

const createBatchTable2 = `CREATE UNLOGGED TABLE IF NOT EXISTS
  "other"."batch_items" AS
  SELECT "id"
  FROM "items"
  WHERE false
`

const createBatchTable3 = `CREATE UNLOGGED TABLE IF NOT EXISTS
  "pgroll"."batch_items" AS
  SELECT "id", "zip"
  FROM "items"
  WHERE false
`

const createBatchTable4 = `CREATE UNLOGGED TABLE IF NOT EXISTS
  "pgroll"."batch_products" AS
  SELECT "id", "zip"
  FROM "products"
  WHERE false
`

const selectBatchInto1 = `INSERT INTO "pgroll"."batch_items"
  ("id")
  SELECT "id"
  FROM "items"
  ORDER BY "id"
  LIMIT 1000
`

const selectBatchInto2 = `INSERT INTO "pgroll"."batch_items"
  ("id")
  SELECT "id"
  FROM "items"
  WHERE ("id") > ('100')
  ORDER BY "id"
  LIMIT 1000
`

const selectBatchInto3 = `INSERT INTO "pgroll"."batch_products"
  ("id", "zip")
  SELECT "id", "zip"
  FROM "products"
  WHERE ("id", "zip") > ('100', 'abc')
  ORDER BY "id", "zip"
  LIMIT 2000
`

const updateBatch1 = `WITH update AS
(
  UPDATE "items"
  SET "id" = "items"."id"
  FROM "pgroll"."batch_items" AS batch
  WHERE "items"."id" = batch."id"
  RETURNING "items"."id"
)
SELECT LAST_VALUE("id") OVER()
FROM update
`

const updateBatch2 = `WITH update AS
(
  UPDATE "products"
  SET "id" = "products"."id", "zip" = "products"."zip"
  FROM "pgroll"."batch_products" AS batch
  WHERE "products"."id" = batch."id" AND "products"."zip" = batch."zip"
  RETURNING "products"."id", "products"."zip"
)
SELECT LAST_VALUE("id") OVER(), LAST_VALUE("zip") OVER()
FROM update
`
