# Rename table

A rename table operation renames a table.

**rename table** operations have this structure:

```json
{
  "rename_table": {
    "from": "old column name",
    "to": "new column name"
  }
}
```

## Example **rename table** migrations:

- [04_rename_table.json](../../examples/04_rename_table.json)
