# Add foreign key

Add foreign key operations add a foreign key constraint to a column.

**add foreign key** constraints have this structure:

```json
{
  "alter_column": {
    "table": "table name",
    "column": "column name",
    "references": {
      "name": "name of foreign key reference",
      "table": "name of referenced table",
      "column": "name of referenced column",
      "on_delete": "ON DELETE behaviour, can be CASCADE, SET NULL, RESTRICT, or NO ACTION. Default is NO ACTION"
    },
    "up": "SQL expression",
    "down": "SQL expression"
  }
}
```

## Example **add foreign key** migrations:

- [21_add_foreign_key_constraint.json](../../../examples/21_add_foreign_key_constraint.json)
