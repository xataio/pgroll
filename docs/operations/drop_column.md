# Drop column

## Structure

A drop column operation drops a column from an existing table.

**drop column** operations have this structure:

```json
{
  "drop_column": {
    "table": "name of table",
    "column": "name of column to drop",
    "down": "SQL expression"
  }
}
```

The `down` field above is required in order to backfill the previous version of the schema during an active migration. For instance, in our [example](../../examples/09_drop_column.json), you can see that if a new row is inserted against the new schema without a `price` column, the old schema `price` column will be set to `0`.

## Examples

- [09_drop_column.json](../../examples/09_drop_column.json)
