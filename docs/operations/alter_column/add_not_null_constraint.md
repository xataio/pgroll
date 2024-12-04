# Add not null constraint

Add not null operations add a `NOT NULL` constraint to a column.

**add not null** operations have this structure:

```json
{
  "alter_column": {
    "table": "table name",
    "column": "column name",
    "nullable": false,
    "up": "SQL expression",
    "down": "SQL expression"
  }
}
```

## Example **add not null** migrations:

- [16_set_nullable.json](../../../examples/16_set_nullable.json)
