# Change type

## Structure

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

## Examples

- [18_change_column_type.json](../../../examples/18_change_column_type.json)
