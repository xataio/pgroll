# Change type

A change type operation changes the type of a column.

**change type** operations have this structure:

```json
{
  "alter_column": {
    "table": "table name",
    "column": "column name",
    "type": "new type of column",
    "up": "SQL expression",
    "down": "SQL expression"
  }
}
```

## Example **change type** migrations:

- [18_change_column_type.json](../../../examples/18_change_column_type.json)
