# Drop multi-column constraint

A drop constraint operation drops a multi-column constraint from an existing table.

Only `CHECK`, `FOREIGN KEY`, and `UNIQUE` constraints can be dropped.

**drop multi-column constraint** operations have this structure:

```json
{
  "drop_multicolumn_constraint": {
    "table": "name of table",
    "name": "name of constraint to drop",
    "up": {
      "column1": "up SQL expressions for each column covered by the constraint",
      ...
    },
    "down": {
      "column1": "down SQL expressions for each column covered by the constraint",
      ...
    }
  }
}
```

## Example **drop multi-column constraint** migrations:

- [48_drop_tickets_check.json](../../examples/48_drop_tickets_check.json)
