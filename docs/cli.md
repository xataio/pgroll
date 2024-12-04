# Command line reference

This document describes the `pgroll` CLI and its subcommands.

The `pgroll` CLI offers the following subcommands:
* [init](#init)
* [start](#start)
* [complete](#complete)
* [rollback](#rollback)
* [status](#status)
* [migrate](#migrate)
* [latest](#latest)
* [pull](#pull)

The `pgroll` CLI has the following top-level flags:
* `--postgres-url`: The URL of the postgres instance against which migrations will be run.
* `--schema`: The Postgres schema in which migrations will be run (default `"public"`).
* `--pgroll-schema`: The Postgres schema in which `pgroll` will store its internal state (default: `"pgroll"`). One `--pgroll-schema` may be used safely with multiple `--schema`s.
* `--lock-timeout`: The Postgres `lock_timeout` value to use for all `pgroll` DDL operations, specified in milliseconds (default `500`).
* `--role`: The Postgres role to use for all `pgroll` DDL operations (default: `""`, which doesn't set any role).

Each of these flags can also be set via an environment variable:
* `PGROLL_PG_URL`
* `PGROLL_SCHEMA`
* `PGROLL_STATE_SCHEMA`
* `PGROLL_LOCK_TIMEOUT`
* `PGROLL_ROLE`

The CLI flag takes precedence if a flag is set via both an environment variable and a CLI flag.

### Init

`pgroll init` initializes `pgroll` for first use.

```
$ pgroll init
```

This will create a new schema in the database called `pgroll` (or whatever value is specified with the `--pgroll-schema` switch). 

The tables and functions in this schema store `pgroll`'s internal state and are not intended to be modified outside of `pgroll` CLI.

### Start

`pgroll start` starts a `pgroll` migration:

```
$ pgroll start sql/03_add_column.json
```

This starts the migration defined in the `sql/03_add_column.json` file.

After starting a migration there will be two schema versions in the database; one for the old schema before the migration (e.g. `public_02_create_table`) and one for the new version with the schema changes (e.g. `public_03_add_column`).  Each of these schemas merely contains views on the tables in the `public` schema.

#### Using `pgroll start` with the `--complete` flag

A migration can be started and completed with one command by specifying the `--complete` flag:

```
$ pgroll start sql/03_add_column.json --complete
```

This is equivalent to running `pgroll start` immediately followed by `pgroll complete`.

:warning: Using the `--complete` flag is appropriate only when there are no applications running against the old database schema. In most cases, the recommended workflow is to run `pgroll start`, then gracefully shut down old applications before running `pgroll complete` as a separate step.

### Complete

`pgroll complete` completes a `pgroll` migration, removing the previous schema and leaving only the latest schema.

```
$ pgroll complete
```

This completes the most recently started migration. 

Running `pgroll complete` when there is no migration in progress is a no-op.

Completing a `pgroll` migration removes the previous schema version from the database (e.g. `public_02_create_table`), leaving only the latest version of the schema (e.g. `public_03_add_column`). At this point, any temporary columns and triggers created on the affected tables in the `public` schema will also be cleaned up, leaving the table schema in its final state. Note that the real schema (e.g. `public`) should never be used directly by the client as that is not safe; instead, clients should use the schemas with versioned views (e.g. `public_03_add_column`).

:warning: Before running `pgroll complete` ensure that all applications that depend on the old version of the database schema are no longer live. Prematurely running `pgroll complete` can cause downtime of old application instances that depend on the old schema.

### Rollback

`pgroll rollback` rolls back the currently active migration.

```
$ pgroll rollback
```

This rolls back the currently active migration (an active migration is one that has been started but not yet completed). 

Rolling back a `pgroll` migration means removing the new schema version. The old schema version was still present throughout the migration period and does not require modification.

Migrations cannot be rolled back once completed. Attempting to roll back a migration that has already been completed is a no-op.

:warning: Before running `pgroll rollback` ensure that any new versions of applications that depend on the new database schema are no longer live. Prematurely running `pgroll rollback` can cause downtime of new application instances that depend on the new schema.


### Migrate

`pgroll migrate` applies all outstanding migrations from a directory to the target database.

Assuming that migrations up to and including migration `40_create_enum_type` from the [example migrations](https://github.com/xataio/pgroll/tree/main/examples) directory have been applied, running:

```
$ pgroll migrate examples/
```

will apply migrations from `41_add_enum_column` onwards to the target database.

If the `--complete` flag is passed to `pgroll migrate` the final migration to be applied will be completed. Otherwise the final migration will be left active (started but not completed).

### Latest

`pgroll latest` prints the latest schema version in either the target database or a local directory of migration files. 

By default, `pgroll latest` prints the latest version in the target database. Use the `--local` flag to print the latest version in a local directory of migration files instead.

In both cases, the `--with-schema` flag can be used to prefix the latest version with the schema name.

#### Database

Assuming that the [example migrations](https://github.com/xataio/pgroll/tree/main/examples) have been applied to the `public` schema in the target database, running:

```
$ pgroll latest 
```

will print the latest version in the target database:

```
45_add_table_check_constraint
```

The exact output will vary as the `examples/` directory is updated.

#### Local

Assuming that the [example migrations](https://github.com/xataio/pgroll/tree/main/examples) are on disk in a directory called `examples`, running:

```
$ pgroll latest --local examples/
```

will print the latest migration in the directory:

```
45_add_table_check_constraint
```

The exact output will vary as the `examples/` directory is updated.

### Status

`pgroll status` shows the current status of `pgroll` within a given schema:

```
$ pgroll status
```
```json
{
  "Schema": "public",
  "Version": "27_drop_unique_constraint",
  "Status": "Complete"
}
```

The status field can be one of the following values:
* `"No migrations"` - no migrations have been applied in this schema yet.
* `"In progress"` - a migration has been started, but not yet completed. 
* `"Complete"` - the most recent migration was completed.

The `Version` field gives the name of the latest schema version. 

If a migration is `In progress` the schemas for both the latest version indicated by the `Version` field and the previous version will exist in the database.

If a migration is `Complete` only the latest version of the schema will exist in the database.

The top-level `--schema` flag can be used to view the status of `pgroll` in a different schema:

```
$ pgroll status --schema schema_a
```
```json
{
  "Schema": "schema_a",
  "Version": "01_create_tables",
  "Status": "Complete"
}
```

### Pull

`pgroll pull` pulls the complete schema history of applied migrations from the target database and writes the migrations to disk.

Assuming that all [example migrations](https://github.com/xataio/pgroll/tree/main/examples) have been applied, running:

```
$ pgroll pull migrations/
```

will write the complete schema history as `.json` files to the `migrations/` directory:

```
$ ls migrations/

01_create_tables.json
02_create_another_table.json
03_add_column_to_products.json
04_rename_table.json
05_sql.json
06_add_column_to_sql_table.json
...
```

The command takes an optional `--with-prefixes` flag which will write each filename prefixed with its position in the schema history:

```
$ ls migrations/

0001_01_create_tables.json
0002_02_create_another_table.json
0003_03_add_column_to_products.json
0004_04_rename_table.json
0005_05_sql.json
0006_06_add_column_to_sql_table.json
...
```
The `--with-prefixes` flag ensures that files are sorted lexicographically by their time of application.

If the directory specified as the required argument to `pgroll pull` does not exist, `pgroll pull` will create it.
