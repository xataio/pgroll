# Change default

A change default operation changes the default value of a column.

**change default** operations have this structure:

```json
{
  "alter_column": {
    "table": "table name",
    "column": "column name",
    "default": "new default value" | null,
    "up": "SQL expression",
    "down": "SQL expression"
  }
}
```

## Example **change default** migrations:

- [35_alter_column_multiple.json](../../../examples/35_alter_column_multiple.json)
- [46_alter_column_drop_default](../../../examples/46_alter_column_drop_default.json)
