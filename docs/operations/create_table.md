# Create table

## Structure

A create table operation creates a new table in the database.

**create table** operations have this structure:

```json
{
  "create_table": {
    "name": "name of new table",
    "columns": [...]
  }
}
```

where each `column` is defined as:

```json
{
  "name": "column name",
  "type": "postgres type",
  "comment": "postgres comment for the column",
  "nullable": true|false,
  "unique": true|false,
  "pk": true|false,
  "default": "default value",
  "check": {
    "name": "name of check constraint"
    "constraint": "constraint expression"
  },
  "references": {
    "name": "name of foreign key constraint",
    "table": "name of referenced table",
    "column": "name of referenced column",
    "on_delete": "ON DELETE behaviour, can be CASCADE, SET NULL, RESTRICT, or NO ACTION. Default is NO ACTION",
  }
},
```

Default values are subject to the usual rules for quoting SQL expressions. In particular, string literals should be surrounded with single quotes.

## Examples

- [01_create_tables.json](../../examples/01_create_tables.json)
- [02_create_another_table.json](../../examples/02_create_another_table.json)
- [08_create_fruits_table.json](../../examples/08_create_fruits_table.json)
- [12_create_employees_table.json](../../examples/12_create_employees_table.json)
- [14_add_reviews_table.json](../../examples/14_add_reviews_table.json)
- [19_create_orders_table.json](../../examples/19_create_orders_table.json)
- [20_create_posts_table.json](../../examples/20_create_posts_table.json)
- [25_add_table_with_check_constraint.json](../../examples/25_add_table_with_check_constraint.json)
- [28_different_defaults.json](../../examples/28_different_defaults.json)
