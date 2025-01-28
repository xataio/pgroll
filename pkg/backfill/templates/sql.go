// SPDX-License-Identifier: Apache-2.0

package templates

const SQL = `WITH batch AS
(
  SELECT {{ commaSeparate (quoteIdentifiers .PrimaryKey) }}
  FROM {{ .TableName | qi}}
  WHERE (
    {{ .StateSchema | qi }}.b_follows_a(xmin::text::bigint, {{ .TransactionID }}) OR
    {{ .StateSchema | qi }}.b_follows_a(xmin::text::bigint, {{ .StateSchema | qi }}.frozen_xid({{ .Schema | ql }}, {{ .TableName | ql }})::text::bigint)
  )
  {{ if .LastValue -}}
  AND ({{ commaSeparate (quoteIdentifiers .PrimaryKey) }}) > ({{ commaSeparate (quoteLiterals .LastValue) }})
  {{ end -}}
  ORDER BY {{ commaSeparate (quoteIdentifiers .PrimaryKey) }}
  LIMIT {{ .BatchSize }}
  FOR NO KEY UPDATE
),
update AS
(
  UPDATE {{ .TableName | qi }}
  SET {{ updateSetClause .TableName .PrimaryKey }}
  FROM batch
  WHERE {{ updateWhereClause .TableName .PrimaryKey }}
  RETURNING {{ updateReturnClause .TableName .PrimaryKey }}
)
SELECT {{ selectLastValue .PrimaryKey }}
FROM update
`
