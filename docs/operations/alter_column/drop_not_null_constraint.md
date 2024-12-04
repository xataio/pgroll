# Drop not null constraint

Drop not null operations drop a `NOT NULL` constraint from a column.

**drop not null** operations have this structure:

```json
{
  "alter_column": {
    "table": "table name",
    "column": "column name",
    "nullable": true,
    "up": "SQL expression",
    "down": "SQL expression"
  }
}
```

## Example **drop not null** migrations:

- [31_unset_not_null.json](../../../examples/31_unset_not_null.json)
