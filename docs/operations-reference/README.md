# Operations reference

`pgroll` migrations are specified as JSON files. All migrations follow the same basic structure:

```json
{
  "name": "0x_migration_name",
  "operations": [...]
}
```

See the [examples](../../examples) directory for examples of each kind of operation.

`pgroll` supports the following migration operations:

- [Add column](./add_column.md)
- [Alter column](./alter_column.md)
  - [Rename column](./rename_column.md)
  - [Change type](./change_type.md)
  - [Change default](./change_default.md)
  - [Change comment](./change_comment.md)
  - [Add check constraint](./add_check_constraint.md)
  - [Add foreign key](./add_foreign_key.md)
  - [Add not null constraint](./add_not_null_constraint.md)
  - [Drop not null constraint](./drop_not_null_constraint.md)
  - [Add unique constraint](./add_unique_constraint.md)
- [Create index](./create_index.md)
- [Create table](./create_table.md)
- [Create constraint](./create_constraint.md)
- [Drop column](./drop_column.md)
- [Drop constraint](./drop_constraint.md)
- [Drop multi-column constraint](./drop_multi_column_constraint.md)
- [Drop index](./drop_index.md)
- [Drop table](./drop_table.md)
- [Raw SQL](./raw_sql.md)
- [Rename table](./rename_table.md)
- [Rename constraint](./rename_constraint.md)
- [Set replica identity](./set_replica_identity.md)
