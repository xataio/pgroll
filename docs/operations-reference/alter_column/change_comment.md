# Change comment

A change comment operation changes the comment on a column.

**change comment** operations have this structure:

```json
{
  "alter_column": {
    "table": "table name",
    "column": "column name",
    "comment": "new comment for column" | null,
    "up": "SQL expression",
    "down": "SQL expression"
  }
}
```

## Example **change comment** migrations:

- [35_alter_column_multiple.json](../../../examples/35_alter_column_multiple.json)
- [36_set_comment_to_null.json](../../../examples/36_set_comment_to_null.json)
