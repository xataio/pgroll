// SPDX-License-Identifier: Apache-2.0

package templates

// CreateBatchTable is a template for creating a batch table. The batch table
// is used to store the primary key values of each batch of rows to be
// backfilled.
const CreateBatchTable = `CREATE UNLOGGED TABLE IF NOT EXISTS
  {{ .StateSchema | qi }}.{{ printf "%s%s" .BatchTablePrefix .TableName | qi}} AS
  SELECT {{ commaSeparate (quoteIdentifiers .IDColumns) }}
  FROM {{ .TableName | qi }}
  WHERE false
`

// SelectBatchInto is a template for selecting the primary key values of the
// rows to be backfilled and inserting them into the batch table.
const SelectBatchInto = `INSERT INTO {{ .StateSchema | qi }}.{{ printf "%s%s" .BatchTablePrefix .TableName | qi }}
  ({{ commaSeparate (quoteIdentifiers .PrimaryKey) }})
  SELECT {{ commaSeparate (quoteIdentifiers .PrimaryKey) }}
  FROM {{ .TableName | qi }}
  {{ if .LastValue | len -}}
  WHERE ({{ commaSeparate (quoteIdentifiers .PrimaryKey) }}) > ({{ commaSeparate (quoteLiterals .LastValue) }})
  {{ end -}}
  ORDER BY {{ commaSeparate (quoteIdentifiers .PrimaryKey) }}
  LIMIT {{ .BatchSize }}
`

// UpdateBatch is a template for updating those rows in the target table having
// primary keys in the batch table. Each update sets the target table rows'
// primary keys to themselves, triggering a no-op update to the row.
const UpdateBatch = `WITH update AS
(
  UPDATE {{ .TableName | qi }}
  SET {{ updateSetClause .TableName .PrimaryKey }}
  FROM {{ .StateSchema | qi }}.{{ printf "%s%s" .BatchTablePrefix .TableName | qi }} AS batch
  WHERE {{ updateWhereClause .TableName .PrimaryKey }}
  RETURNING {{ updateReturnClause .TableName .PrimaryKey }}
)
SELECT {{ selectLastValue .PrimaryKey }}
FROM update
`
