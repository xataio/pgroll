# Create constraint

A create constraint operation adds a new constraint to an existing table.

`UNIQUE`, `CHECK` and `FOREIGN KEY` constraints are supported.

Required fields: `name`, `table`, `type`, `up`, `down`.

**create constraint** operations have this structure:

```json
{
  "create_constraint": {
    "table": "name of table",
    "name": "my_unique_constraint",
    "columns": ["column1", "column2"],
    "type": "unique"| "check" | "foreign_key",
    "check": "SQL expression for CHECK constraint",
    "references": {
      "name": "name of foreign key reference",
      "table": "name of referenced table",
      "columns": "[names of referenced columns]",
      "on_delete": "ON DELETE behaviour, can be CASCADE, SET NULL, RESTRICT, or NO ACTION. Default is NO ACTION",
    },
    "up": {
      "column1": "up SQL expressions for each column covered by the constraint",
      ...
    },
    "down": {
      "column1": "up SQL expressions for each column covered by the constraint",
      ...
    }
  }
}
```

## Example **create constraint** migrations:

- [44_add_table_unique_constraint.json](../../examples/44_add_table_unique_constraint.json)
- [45_add_table_check_constraint.json](../../examples/45_add_table_check_constraint.json)
- [46_add_table_foreign_key_constraint.json](../../examples/46_add_table_foreign_key_constraint.json)