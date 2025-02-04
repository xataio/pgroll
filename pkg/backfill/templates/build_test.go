// SPDX-License-Identifier: Apache-2.0

package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchStatementBuilder(t *testing.T) {
	tests := map[string]struct {
		config   BatchConfig
		expected string
	}{
		"single identity column no last value": {
			config: BatchConfig{
				TableName:           "table_name",
				PrimaryKey:          []string{"id"},
				NeedsBackfillColumn: "_pgroll_needs_backfill",
				BatchSize:           10,
			},
			expected: expectSingleIDColumnNoLastValue,
		},
		"multiple identity columns no last value": {
			config: BatchConfig{
				TableName:           "table_name",
				PrimaryKey:          []string{"id", "zip"},
				NeedsBackfillColumn: "_pgroll_needs_backfill",
				BatchSize:           10,
			},
			expected: multipleIDColumnsNoLastValue,
		},
		"single identity column with last value": {
			config: BatchConfig{
				TableName:           "table_name",
				PrimaryKey:          []string{"id"},
				NeedsBackfillColumn: "_pgroll_needs_backfill",
				LastValue:           []string{"1"},
				BatchSize:           10,
			},
			expected: singleIDColumnWithLastValue,
		},
		"multiple identity columns with last value": {
			config: BatchConfig{
				TableName:           "table_name",
				PrimaryKey:          []string{"id", "zip"},
				NeedsBackfillColumn: "_pgroll_needs_backfill",
				LastValue:           []string{"1", "1234"},
				BatchSize:           10,
			},
			expected: multipleIDColumnsWithLastValue,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			actual, err := BuildSQL(test.config)
			assert.NoError(t, err)

			assert.Equal(t, test.expected, actual)
		})
	}
}

const expectSingleIDColumnNoLastValue = `WITH batch AS
(
  SELECT "id"
  FROM "table_name"
  WHERE "_pgroll_needs_backfill" = true
  ORDER BY "id"
  LIMIT 10
  FOR NO KEY UPDATE
),
update AS
(
  UPDATE "table_name"
  SET "id" = "table_name"."id"
  FROM batch
  WHERE "table_name"."id" = batch."id"
  RETURNING "table_name"."id"
)
SELECT LAST_VALUE("id") OVER()
FROM update
`

const multipleIDColumnsNoLastValue = `WITH batch AS
(
  SELECT "id", "zip"
  FROM "table_name"
  WHERE "_pgroll_needs_backfill" = true
  ORDER BY "id", "zip"
  LIMIT 10
  FOR NO KEY UPDATE
),
update AS
(
  UPDATE "table_name"
  SET "id" = "table_name"."id", "zip" = "table_name"."zip"
  FROM batch
  WHERE "table_name"."id" = batch."id" AND "table_name"."zip" = batch."zip"
  RETURNING "table_name"."id", "table_name"."zip"
)
SELECT LAST_VALUE("id") OVER(), LAST_VALUE("zip") OVER()
FROM update
`

const singleIDColumnWithLastValue = `WITH batch AS
(
  SELECT "id"
  FROM "table_name"
  WHERE "_pgroll_needs_backfill" = true
  AND ("id") > ('1')
  ORDER BY "id"
  LIMIT 10
  FOR NO KEY UPDATE
),
update AS
(
  UPDATE "table_name"
  SET "id" = "table_name"."id"
  FROM batch
  WHERE "table_name"."id" = batch."id"
  RETURNING "table_name"."id"
)
SELECT LAST_VALUE("id") OVER()
FROM update
`

const multipleIDColumnsWithLastValue = `WITH batch AS
(
  SELECT "id", "zip"
  FROM "table_name"
  WHERE "_pgroll_needs_backfill" = true
  AND ("id", "zip") > ('1', '1234')
  ORDER BY "id", "zip"
  LIMIT 10
  FOR NO KEY UPDATE
),
update AS
(
  UPDATE "table_name"
  SET "id" = "table_name"."id", "zip" = "table_name"."zip"
  FROM batch
  WHERE "table_name"."id" = batch."id" AND "table_name"."zip" = batch."zip"
  RETURNING "table_name"."id", "table_name"."zip"
)
SELECT LAST_VALUE("id") OVER(), LAST_VALUE("zip") OVER()
FROM update
`
