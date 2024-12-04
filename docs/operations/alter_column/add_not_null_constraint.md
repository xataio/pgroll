# Add not null constraint

## Structure

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

## Examples

- [16_set_nullable.json](../../../examples/16_set_nullable.json)
