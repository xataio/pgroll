# Drop constraint

## Structure

:warning: The **drop constraint** operation is deprecated. Please use the [drop multi-column constraint](#drop-multi-column-constraint) operation instead. The **drop_constraint** operation will be removed in a future release of `pgroll`. :warning:

A drop constraint operation drops a single-column constraint from an existing table.

Only `CHECK`, `FOREIGN KEY`, and `UNIQUE` constraints can be dropped.

**drop constraint** operations have this structure:

```json
{
  "drop_constraint": {
    "table": "name of table",
    "name": "name of constraint to drop",
    "up": "SQL expression",
    "down": "SQL expression"
  }
}
```

## Examples

- [23_drop_check_constraint.json](../../examples/23_drop_check_constraint.json)
- [24_drop_foreign_key_constraint.json](../../examples/24_drop_foreign_key_constraint.json)
- [27_drop_unique_constraint.json](../../examples/27_drop_unique_constraint.json)
