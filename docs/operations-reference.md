## Operations reference

`pgroll` migrations are specified as JSON files. All migrations follow the same basic structure:

```json
{
  "name": "0x_migration_name",
  "operations": [...]
}
```

See the [examples](../examples) directory for examples of each kind of operation.

`pgroll` supports the following migration operations:

* [Add column](#add-column)
* [Alter column](#alter-column)
    * [Rename column](#rename-column)
    * [Change type](#change-type)
    * [Change default](#change-default)
    * [Change comment](#change-comment)
    * [Add check constraint](#add-check-constraint)
    * [Add foreign key](#add-foreign-key)
    * [Add not null constraint](#add-not-null-constraint)
    * [Drop not null constraint](#drop-not-null-constraint)
    * [Add unique constraint](#add-unique-constraint)
* [Create index](#create-index)
* [Create table](#create-table)
* [Create constraint](#create-constraint)
* [Drop column](#drop-column)
* [Drop constraint](#drop-constraint)
* [Drop multi-column constraint](#drop-multi-column-constraint)
* [Drop index](#drop-index)
* [Drop table](#drop-table)
* [Raw SQL](#raw-sql)
* [Rename table](#rename-table)
* [Rename constraint](#rename-constraint)
* [Set replica identity](#set-replica-identity)

### Add column

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

Example **add column** migrations:

* [03_add_column.json](../examples/03_add_column.json)
* [06_add_column_to_sql_table.json](../examples/06_add_column_to_sql_table.json)
* [17_add_rating_column.json](../examples/17_add_rating_column.json)
* [26_add_column_with_check_constraint.json](../examples/26_add_column_with_check_constraint.json)
* [30_add_column_simple_up.json](../examples/30_add_column_simple_up.json)
* [41_add_enum_column.json](../examples/41_add_enum_column.json)

### Alter column

An alter column operation alters the properties of a column. The operation supports several sub-operations, described below.

An alter column operation may contain multiple sub-operations. For example, a single alter column operation may rename a column, change its type, and add a check constraint.

#### Rename column

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

Example **rename column** migrations:

* [13_rename_column.json](../examples/13_rename_column.json)

#### Change type

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

Example **change type** migrations:

* [18_change_column_type.json](../examples/18_change_column_type.json)

#### Change default

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

Example **change default** migrations:

* [35_alter_column_multiple.json](../examples/35_alter_column_multiple.json)
* [46_alter_column_drop_default](../examples/46_alter_column_drop_default.json)

#### Change comment

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

* [35_alter_column_multiple.json](../examples/35_alter_column_multiple.json)
* [36_set_comment_to_null.json](../examples/36_set_comment_to_null.json)

#### Add check constraint

An add check constraint operation adds a `CHECK` constraint to a column.

**add check constraint** migrations have this structure:

```json
{
  "alter_column": {
    "table": "table name",
    "column": "column name",
    "check": {
      "name": "check constraint name",
      "constraint": "constraint expression"
    },
    "up": "SQL expression",
    "down": "SQL expression"
  }
}
```

Example **add check constraint** migrations:

* [22_add_check_constraint.json](../examples/22_add_check_constraint.json)

#### Add foreign key

Add foreign key operations add a foreign key constraint to a column.

**add foreign key** constraints have this structure:

```json
{
  "alter_column": {
    "table": "table name",
    "column": "column name",
    "references": {
      "name": "name of foreign key reference",
      "table": "name of referenced table",
      "column": "name of referenced column",
      "on_delete": "ON DELETE behaviour, can be CASCADE, SET NULL, RESTRICT, or NO ACTION. Default is NO ACTION",
    },
    "up": "SQL expression",
    "down": "SQL expression"
  }
}
```

Example **add foreign key** migrations:

* [21_add_foreign_key_constraint.json](../examples/21_add_foreign_key_constraint.json)

#### Add not null constraint

Add not null operations add a `NOT NULL` constraint to a column.

**add not null** operations have this structure:

```json
{
  "alter_column": {
    "table": "table name",
    "column": "column name",
    "nullable": false,
    "up": "SQL expression",
    "down": "SQL expression"
  }
}
```

Example **add not null** migrations:

* [16_set_nullable.json](../examples/16_set_nullable.json)

#### Drop not null constraint

Drop not null operations drop a `NOT NULL` constraint from a column.

**drop not null** operations have this structure:

```json
{
  "alter_column": {
    "table": "table name",
    "column": "column name",
    "nullable": true,
    "up": "SQL expression",
    "down": "SQL expression"
  }
}
```

Example **drop not null** migrations:

* [31_unset_not_null.json](../examples/31_unset_not_null.json)

#### Add unique constraint

Add unique operations add a `UNIQUE` constraint to a column.

**add unique** operations have this structure:

```json
{
  "alter_column": {
    "table": "table name",
    "column": "column name",
    "unique": {
      "name": "name of unique constraint"
    },
    "up": "SQL expression",
    "down": "SQL expression"
  }
}
```

Example **add unique** migrations:

* [15_set_column_unique.json](../examples/15_set_column_unique.json)

### Create index

A create index operation creates a new index on a set of columns.

**create index** operations have this structure:

```json
{
  "create_index": {
    "table": "name of table on which to define the index",
    "name": "index name",
    "columns": [ "names of columns on which to define the index" ]
    "predicate": "conditional expression for defining a partial index",
    "method": "btree"
  }
}
```

The field `method` can be `btree`, `hash`, `gist`, `spgist`, `gin`, `brin`.
You can also specify storage parameters for the index in `storage_parameters`.
To create a unique index set `unique` to `true`.

Example **create index** migrations:

* [10_create_index.json](../examples/10_create_index.json)
* [37_create_partial_index.json](../examples/37_create_partial_index.json)
* [38_create_hash_index_with_fillfactor.json](../examples/38_create_hash_index_with_fillfactor.json)
* [42_create_unique_index.json](../examples/42_create_unique_index.json)

### Create table

A create table operation creates a new table in the database.

**create table** operations have this structure:

```json
{
  "create_table": {
    "name": "name of new table",
    "columns": [...]
    ]
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
  "default": "default value"
  "check": {
    "name": "name of check constraint"
    "constraint": "constraint expression"
  }
  "references": {
    "name": "name of foreign key constraint"
    "table": "name of referenced table"
    "column": "name of referenced column"
    "on_delete": "ON DELETE behaviour, can be CASCADE, SET NULL, RESTRICT, or NO ACTION. Default is NO ACTION",
  }
},
```

Default values are subject to the usual rules for quoting SQL expressions. In particular, string literals should be surrounded with single quotes.

Example **create table** migrations:

* [01_create_tables.json](../examples/01_create_tables.json)
* [02_create_another_table.json](../examples/02_create_another_table.json)
* [08_create_fruits_table.json](../examples/08_create_fruits_table.json)
* [12_create_employees_table.json](../examples/12_create_employees_table.json)
* [14_add_reviews_table.json](../examples/14_add_reviews_table.json)
* [19_create_orders_table.json](../examples/19_create_orders_table.json)
* [20_create_posts_table.json](../examples/20_create_posts_table.json)
* [25_add_table_with_check_constraint.json](../examples/25_add_table_with_check_constraint.json)
* [28_different_defaults.json](../examples/28_different_defaults.json)

### Create constraint

A create constraint operation adds a new constraint to an existing table.

`UNIQUE`, `CHECK` and `FOREIGN KEY` constraints are supported.

Required fields: `name`, `table`, `type`, `up`, `down`.

**create constraint** operations have this structure:

```json
{
  "create_constraint": {
    "table": "name of table",
    "name": "my_unique_constraint",
    "columns": ["column1", "column2"],
    "type": "unique"| "check" | "foreign_key",
    "check": "SQL expression for CHECK constraint",
    "references": {
      "name": "name of foreign key reference",
      "table": "name of referenced table",
      "columns": "[names of referenced columns]",
      "on_delete": "ON DELETE behaviour, can be CASCADE, SET NULL, RESTRICT, or NO ACTION. Default is NO ACTION",
    },
    "up": {
      "column1": "up SQL expressions for each column covered by the constraint",
      ...
    },
    "down": {
      "column1": "up SQL expressions for each column covered by the constraint",
      ...
    }
  }
}
```

Example **create constraint** migrations:

* [44_add_table_unique_constraint.json](../examples/44_add_table_unique_constraint.json)
* [45_add_table_check_constraint.json](../examples/45_add_table_check_constraint.json)
* [46_add_table_foreign_key_constraint.json](../examples/46_add_table_foreign_key_constraint.json)

### Drop column

A drop column operation drops a column from an existing table.

**drop column** operations have this structure:

```json
{
  "drop_column": {
    "table": "name of table",
    "column": "name of column to drop",
    "down": "SQL expression"
  }
}
```

The `down` field above is required in order to backfill the previous version of the schema during an active migration. For instance, in our [example](../examples/09_drop_column.json), you can see that if a new row is inserted against the new schema without a `price` column, the old schema `price` column will be set to `0`.

Example **drop column** migrations:

* [09_drop_column.json](../examples/09_drop_column.json)

### Drop constraint

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

Example **drop constraint** migrations:

* [23_drop_check_constraint.json](../examples/23_drop_check_constraint.json)
* [24_drop_foreign_key_constraint.json](../examples/24_drop_foreign_key_constraint.json)
* [27_drop_unique_constraint.json](../examples/27_drop_unique_constraint.json)

### Drop multi-column constraint

A drop constraint operation drops a multi-column constraint from an existing table.

Only `CHECK`, `FOREIGN KEY`, and `UNIQUE` constraints can be dropped.

**drop multi-column constraint** operations have this structure:

```json
{
  "drop_multicolumn_constraint": {
    "table": "name of table",
    "name": "name of constraint to drop",
    "up": {
      "column1": "up SQL expressions for each column covered by the constraint",
      ...
    },
    "down": {
      "column1": "down SQL expressions for each column covered by the constraint",
      ...
    }
  }
}
```

Example **drop multi-column constraint** migrations:

* [48_drop_tickets_check.json](../examples/48_drop_tickets_check.json)

### Drop index

A drop index operation drops an index from a table.

**drop index** operations have this structure:

```json
{
  "drop_index": {
    "name": "name of index to drop"
  }
}
```

Example **drop index** migrations:

* [11_drop_index.json](../examples/11_drop_index.json)

### Drop table

A drop table operation drops a table.

**drop table** operations have this structure:

```json
{
  "drop_table": {
    "name": "name of table to drop"
  }
}
```

Example **drop table** migrations:

* [07_drop_table.json](../examples/07_drop_table.json)

### Raw SQL

A raw SQL operation runs arbitrary SQL against the database. This is intended as an 'escape hatch' to allow a migration to perform operations that are otherwise not supported by `pgroll`.

:warning: `pgroll` is unable to guarantee that raw SQL migrations are safe and will not result in application downtime. :warning:

**sql** operations have this structure:

```json
{
  "sql": {
    "up": "SQL expression",
    "down": "SQL expression"
  }
}
```

By default, a `sql` operation cannot run together with other operations in the same migration. This is to ensure pgroll can correctly track the state of the database. However, it is possible to run a `sql` operation together with other operations by setting the `onComplete` flag to `true`.

The `onComplete` flag will make this operation run the `up` expression on the complete phase (instead of the default, which is to run it on the start phase).

`onComplete` flag is incompatible with `down` expression, as `pgroll` does not support running rollback after complete was executed.




```json
{
  "sql": {
    "up": "SQL expression",
    "onComplete": true
  }
}
```

Example **raw SQL** migrations:

* [05_sql.json](../examples/05_sql.json)
* [32_sql_on_complete.json](../examples/32_sql_on_complete.json)


### Rename table

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

Example **rename table** migrations:

* [04_rename_table.json](../examples/04_rename_table.json)


### Rename constraint

A rename constraint operation renames a constraint.

**rename constraint** operations have this structure:

```json
{
  "rename_constraint": {
    "table": "table name",
    "from": "old constraint name",
    "to": "new constraint name"
  }
}
```

Example **rename constraint** migrations:

* [33_rename_constraint.json](../examples/33_rename_constraint.json)


### Set replica identity

A set replica identity operation sets the replica identity for a table. 

**set replica identity** operations have this structure:

```json
{
  "set_replica_identity": {
    "table": "name of the table",
    "identity": {
      "type": "full | default | nothing | index"
      "index": "name of the index, if type is 'index'"
    }
  }
}
```

:warning: A **set replica identity** operation is applied directly to the underlying table on migration start. This means that both versions of the table exposed in the old and new version schemas will have the new replica identity set. :warning:

Example **set replica identity** migrations:

* [29_set_replica_identity.json](../examples/29_set_replica_identity.json)
