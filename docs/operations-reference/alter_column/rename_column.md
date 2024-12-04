# Rename column

A rename column operation renames a column.

**rename column** operations have this structure:

```json
{
  "alter_column": {
    "table": "table name",
    "column": "old column name",
    "name": "new column name"
  }
}
```

## Example **rename column** migrations:

- [13_rename_column.json](../../../examples/13_rename_column.json)
