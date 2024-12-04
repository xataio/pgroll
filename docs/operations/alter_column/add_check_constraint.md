# Add check constraint

An add check constraint operation adds a `CHECK` constraint to a column.

**add check constraint** migrations have this structure:

```json
{
  "alter_column": {
    "table": "table name",
    "column": "column name",
    "check": {
      "name": "check constraint name",
      "constraint": "constraint expression"
    },
    "up": "SQL expression",
    "down": "SQL expression"
  }
}
```

## Example **add check constraint** migrations:

- [22_add_check_constraint.json](../../../examples/22_add_check_constraint.json)
