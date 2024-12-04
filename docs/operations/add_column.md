# Add column

## Structure

An add column operation creates a new column on an existing table.

**add column** operations have this structure:

```json
{
  "add_column": {
    "table": "name of table to which the column should be added",
    "up": "SQL expression",
    "column": {
      "name": "name of column",
      "type": "postgres type",
      "comment": "postgres comment for the column",
      "nullable": true|false,
      "unique": true|false,
      "pk": true|false,
      "default": "default value for the column",
      "check": {
        "name": "name of check constraint",
        "constraint": "constraint expression"
      },
      "references": {
        "name": "name of foreign key constraint",
        "table": "name of referenced table",
        "column": "name of referenced column"
        "on_delete": "ON DELETE behaviour, can be CASCADE, SET NULL, RESTRICT, or NO ACTION. Default is NO ACTION",
      }
    }
  }
}
```

Default values are subject to the usual rules for quoting SQL expressions. In particular, string literals should be surrounded with single quotes.

**NOTE:** As a special case, the `up` field can be omitted when adding `smallserial`, `serial` and `bigserial` columns.

## Examples

- [03_add_column.json](../../examples/03_add_column.json)
- [06_add_column_to_sql_table.json](../../examples/06_add_column_to_sql_table.json)
- [17_add_rating_column.json](../../examples/17_add_rating_column.json)
- [26_add_column_with_check_constraint.json](../../examples/26_add_column_with_check_constraint.json)
- [30_add_column_simple_up.json](../../examples/30_add_column_simple_up.json)
- [41_add_enum_column.json](../../examples/41_add_enum_column.json)
