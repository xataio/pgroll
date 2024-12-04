# Add unique constraint

Add unique operations add a `UNIQUE` constraint to a column.

**add unique** operations have this structure:

```json
{
  "alter_column": {
    "table": "table name",
    "column": "column name",
    "unique": {
      "name": "name of unique constraint"
    },
    "up": "SQL expression",
    "down": "SQL expression"
  }
}
```

## Example **add unique** migrations:

- [15_set_column_unique.json](../../../examples/15_set_column_unique.json)
