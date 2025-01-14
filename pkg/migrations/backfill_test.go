// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchStatementBuilder(t *testing.T) {
	tests := map[string]struct {
		tableName       string
		identityColumns []string
		batchSize       int
		lasValues       []string
		expected        string
	}{
		"single identity column no last value": {
			tableName:       "table_name",
			identityColumns: []string{"id"},
			batchSize:       10,
			expected:        `WITH batch AS (SELECT "id" FROM "table_name"  ORDER BY "id" LIMIT 10 FOR NO KEY UPDATE), update AS (UPDATE "table_name" SET "id" = "table_name"."id" FROM batch WHERE "table_name"."id" = batch."id" RETURNING "table_name"."id") SELECT LAST_VALUE("id") OVER() FROM update`,
		},
		"multiple identity columns no last value": {
			tableName:       "table_name",
			identityColumns: []string{"id", "zip"},
			batchSize:       10,
			expected:        `WITH batch AS (SELECT "id", "zip" FROM "table_name"  ORDER BY "id", "zip" LIMIT 10 FOR NO KEY UPDATE), update AS (UPDATE "table_name" SET "id" = "table_name"."id", "zip" = "table_name"."zip" FROM batch WHERE "table_name"."id" = batch."id" AND "table_name"."zip" = batch."zip" RETURNING "table_name"."id", "table_name"."zip") SELECT LAST_VALUE("id") OVER(), LAST_VALUE("zip") OVER() FROM update`,
		},
		"single identity column with last value": {
			tableName:       "table_name",
			identityColumns: []string{"id"},
			batchSize:       10,
			lasValues:       []string{"1"},
			expected:        `WITH batch AS (SELECT "id" FROM "table_name" WHERE "id" > '1' ORDER BY "id" LIMIT 10 FOR NO KEY UPDATE), update AS (UPDATE "table_name" SET "id" = "table_name"."id" FROM batch WHERE "table_name"."id" = batch."id" RETURNING "table_name"."id") SELECT LAST_VALUE("id") OVER() FROM update`,
		},
		"multiple identity columns with last value": {
			tableName:       "table_name",
			identityColumns: []string{"id", "zip"},
			batchSize:       10,
			lasValues:       []string{"1", "1234"},
			expected:        `WITH batch AS (SELECT "id", "zip" FROM "table_name" WHERE ("id", "zip") > ('1', '1234') ORDER BY "id", "zip" LIMIT 10 FOR NO KEY UPDATE), update AS (UPDATE "table_name" SET "id" = "table_name"."id", "zip" = "table_name"."zip" FROM batch WHERE "table_name"."id" = batch."id" AND "table_name"."zip" = batch."zip" RETURNING "table_name"."id", "table_name"."zip") SELECT LAST_VALUE("id") OVER(), LAST_VALUE("zip") OVER() FROM update`,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			builder := newBatchStatementBuilder(test.tableName, test.identityColumns, test.batchSize)
			actual := builder.buildQuery(test.lasValues)
			assert.Equal(t, test.expected, actual)
		})
	}
}
