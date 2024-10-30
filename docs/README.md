# :books: `pgroll` Documentation

* [Concepts](#concepts)
    * [Migration workflow](#migration-workflow)
    * [Multiple schema versions](#multiple-schema-versions)
    * [Client applications](#client-applications)
* [Installation](#installation)
* [Supported Postgres versions](#supported-postgres-versions)
* [Tutorial](#tutorial)
* [Command line reference](#command-line-reference)
    * [init](#init)
    * [start](#start)
    * [complete](#complete)
    * [rollback](#rollback)
    * [status](#status)
* [Operations reference](#operations-reference)
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
    * [Drop column](#drop-column)
    * [Drop constraint](#drop-constraint)
    * [Drop index](#drop-index)
    * [Drop table](#drop-table)
    * [Raw SQL](#raw-sql)
    * [Rename table](#rename-table)
    * [Rename constraint](#rename-constraint)
    * [Set replica identity](#set-replica-identity)

## Concepts

`pgroll` introduces a few concepts that are important to understand before using the tool.

### Migration workflow

`pgroll` migrations are applied in two steps, following an [expand/contract pattern](https://openpracticelibrary.com/practice/expand-and-contract-pattern/).

![migration flow](img/migration-flow@2x.png)

During the migration start phase, `pgroll` will perform only additive changes to the database schema. This includes: creating new tables, adding new columns, and creating new indexes. In the cases where a required change is not backwards compatible, `pgroll` will take the necessary steps to ensure that the current schema is still valid. For example, if a new column is added to a table with a `NOT NULL` constraint, `pgroll` will backfill the new column with a default value.

After a successful migration start, the database will contain two versions of the schema: the old version and the new version. The old version of the schema is still available to client applications. This allows client applications to be updated to use the new version of the schema without any downtime.

Once all client applications have been updated to use the latest version of the schema, the complete phase can be run. During the complete phase `pgroll` will perform all non-additive changes to the database schema. This includes: dropping tables, dropping columns, and dropping indexes. Effectively breaking the old version of the schema.

### Multiple schema versions

`pgroll` maintains multiple versions of the database schema side-by-side. This is achieved by creating a new Postgres schema for each migration that is applied to the database. The schema will contain views on the underlying tables. These views are used to expose different tables or columns to client applications depending on which version of the schema they are configured to use.

For instance, a rename column migration will create a new schema containing a view on the underlying table with the new column name. This allows for the new version of the schema to become available without breaking existing client applications that are still using the old name. In the migration complete phase, the old schema is dropped and the actual column is renamed (views are updated to point to the new column name automatically).

![multiple schema versions](img/migration-schemas@2x.png)

For other more complex changes, like adding a `NOT NULL` constraint to a column, `pgroll` will duplicate the affected column and backfill it with the values from the old one. For some time the old & new columns will coexist in the same table. This allows for the new version of the schema to expose the column that fulfils the constraint, while the old version still uses the old column. `pgroll` will take care of copying the values from the old column to the new one, and vice versa, as needed, both by executing the backfill or installing triggers to keep the columns in sync during updates.

### Client applications

In order to work with the multiple versioned schema that `pgroll` creates, clients need to be configured to work with one of them. 

This is done by having client applications configure the [search path](https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH) when they connect to the Postgres database.

For example, this fragment for a Go client application shows how to set the `search_path` after a connection is established:

```go
db, err := sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
if err != nil {
    return nil, err
}

searchPath := "public_02_add_assignee_column"
log.Printf("Setting search path to %q", searchPath)
_, err = db.Exec(fmt.Sprintf("SET search_path = %s", pq.QuoteIdentifier(searchPath)))
if err != nil {
    return nil, fmt.Errorf("failed to set search path: %s", err)
}
```

In practice, the `searchPath` variable would be provided to the application as an environment variable.

#### What happens if an application doesn't set the `search_path`?

If an application doesn't set the `search_path` for the connection, the `search_path` defaults to the `public` schema, meaning that the application will be working with the underlying tables directly rather than accessing them through the versioned views.

## Installation

### Binaries

Binaries are available for Linux, macOS & Windows on our [Releases](https://github.com/xataio/pgroll/releases) page.

### From source

To install `pgroll` from source, run the following command:

```sh
go install github.com/xataio/pgroll@latest
```

Note: requires [Go 1.23](https://golang.org/doc/install) or later.

### From package manager - Homebrew

To install `pgroll` with homebrew, run the following command:

```sh
# macOS or Linux
brew tap xataio/pgroll
brew install pgroll
```

## Supported Postgres versions

`pgroll` supports Postgres versions >= 14.

:warning: In Postgres 14, row level security policies on tables are not respected by `pgroll`'s versioned views. This is because `pgroll` is unable to create the views with the `(security_invoker = true)` option, as the ability to do so was added in Postgres 15. If you use RLS in Postgres 14 `pgroll` is likely a poor choice of migration tool. All other `pgroll` features are fully supported across all supported Postgres versions.

## Tutorial

This section will walk you through applying your first migrations using `pgroll`.

We'll apply two migrations to a fresh database and have a look at what `pgroll` does under the hood.

Prerequisites:

* `pgroll` installed and accessible somewhere on your `$PATH`
* A fresh Postgres instance against which to run migrations

A good way to get a throw-away Postgres instance for use in the tutorial is to use [Docker](https://www.docker.com/). Start a Postgres instance in Docker with:

```
docker run --rm --name for-pgroll -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:16
```

The remainder of the tutorial assumes that you have a local Postgres instance accessible on port 5432.

### Initialization

`pgroll` needs to store its own internal state somewhere in the target Postgres database. Initializing `pgroll` configures this store and makes `pgroll` ready for first use:

```
pgroll init
```

You should see a success message indicating that `pgroll` has been configured.

<details>
  <summary>What data does <code>pgroll</code> store?</summary>
  
  `pgroll` stores its data in the `pgroll` schema. In this schema it creates:
  * A `migrations` table containing the version history for each schema in the database
  * Functions to capture the current database schema for a given schema name
  * Triggers to capture DDL statements run outside of `pgroll` migrations
</details>

### First migration

With `pgroll` initialized, let's run our first migration. Here is a migration to create a table:

```json
{
  "name": "01_create_users_table",
  "operations": [
    {
      "create_table": {
        "name": "users",
        "columns": [
          {
            "name": "id",
            "type": "serial",
            "pk": true
          },
          {
            "name": "name",
            "type": "varchar(255)",
            "unique": true
          },
          {
            "name": "description",
            "type": "text",
            "nullable": true
          }
        ]
      }
    }
  ]
}
```

Take this file and save it as `sql/01_create_users_table.json`.

The migration will create a `users` table with three columns. It is equivalent to the following SQL DDL statement:

```sql
CREATE TABLE users(
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) UNIQUE NOT NULL,
  description TEXT
)
```

To apply the migration to the database run:

```
pgroll start sql/01_create_users_table.json --complete 
```

<details>
  <summary>What does the <code>--complete</code> flag do here?</summary>
 
  `pgroll` divides migration application into two steps: **start** and **complete**. During the **start** phase, both old and new versions of the database schema are available to client applications. After the **complete** phase, only the most recent schema is available.

  As this is the first migration there is no old schema to maintain, so the migration can safely be started and completed in one step.

  For more details about `pgroll`'s two-step migration process, see the [Multiple schema versions](#multiple-schema-versions) section.
</details>

Now let's add some users to our new table:

```sql
INSERT INTO users (name, description)
 SELECT
   'user_' || suffix,
   CASE
     WHEN random() < 0.5 THEN 'description for user_' || suffix
     ELSE NULL
   END
 FROM generate_series(1, 100000) AS suffix;
```

Execute this SQL to insert 10^5 users into the `users` table. Roughly half of the users will have descriptions and the other half will have `NULL` descriptions.

### Second migration

Now that we have our `users` table, lets make a non backwards-compatible change to the schema and see how `pgroll` helps us by maintaining the old and new schema versions side by side.

Some of the users in our `users` table have descriptions and others don't. This is because our initial migration set the `description` column as `nullable: true`, allowing some users to have `NULL` values in the description field.

We'd like to change the `users` table to disallow `NULL` values in the `description` field. We also want a `description` to be set explicitly for all new users, so we don't want to specify a default value for the column.

There are two things that make this migration difficult:

* We have existing `NULL` values in our `description` column that need to be updated to something non-`NULL`
* Existing applications using the table are still running and may be inserting more `NULL` descriptions

`pgroll` helps us solve both problems by maintaining old and new versions of the schema side-by-side and copying/rewriting data between them as required.

Here is the `pgroll` migration that will perform the migration to make the `description` column `NOT NULL`:

```json
{
  "name": "02_user_description_set_nullable",
  "operations": [
    {
      "alter_column": {
        "table": "users",
        "column": "description",
        "nullable": false,
        "up": "(SELECT CASE WHEN description IS NULL THEN 'description for ' || name ELSE description END)",
        "down": "description"
      }
    }
  ]
}
```

Save this migration as `sql/02_user_description_set_nullable.json` and start the migration:

```
pgroll start 02_user_description_set_nullable.json
```

After some progress updates you should see a message saying that the migration has been started successfully.

At this point it's useful to look at the table data and schema to see what `pgroll` has done. Let's look at the data first:

```sql
SELECT * FROM users ORDER BY id LIMIT 10
```

You should see something like this:
```
+-----+----------+-------------------------+--------------------------+
| id  | name     | description             | _pgroll_new_description  |
+-----+----------+-------------------------+--------------------------+
| 1   | user_1   | <null>                  | description for user_1   |
| 2   | user_2   | description for user_2  | description for user_2   |
| 3   | user_3   | <null>                  | description for user_3   |
| 4   | user_4   | description for user_4  | description for user_4   |
| 5   | user_5   | <null>                  | description for user_5   |
| 6   | user_6   | description for user_6  | description for user_6   |
| 7   | user_7   | <null>                  | description for user_7   |
| 8   | user_8   | <null>                  | description for user_8   |
| 9   | user_9   | description for user_9  | description for user_9   |
| 10  | user_10  | description for user_10 | description for user_10  |
```

`pgroll` has added a `_pgroll_new_description` field to the table and populated the field for all rows using the `up` SQL from the `02_user_description_set_nullable.json` file:

```json
"up": "(SELECT CASE WHEN description IS NULL THEN 'description for ' || name ELSE description END)",
```

This has copied over all `description` values into the `_pgroll_new_description` field, rewriting any `NULL` values using the provided SQL.

Now let's look at the table schema:

```sql
DESCRIBE users
```

You should see something like this:

```
+-------------------------+------------------------+-----------------------------------------------------------------+
| Column                  | Type                   | Modifiers                                                       |
+-------------------------+------------------------+-----------------------------------------------------------------+
| id                      | integer                |  not null default nextval('_pgroll_new_users_id_seq'::regclass) |
| name                    | character varying(255) |  not null                                                       |
| description             | text                   |                                                                 |
| _pgroll_new_description | text                   |                                                                 |
+-------------------------+------------------------+-----------------------------------------------------------------+
Indexes:
    "_pgroll_new_users_pkey" PRIMARY KEY, btree (id)
    "_pgroll_new_users_name_key" UNIQUE CONSTRAINT, btree (name)
Check constraints:
    "_pgroll_add_column_check_description" CHECK (_pgroll_new_description IS NOT NULL) NOT VALID
Triggers:
    _pgroll_trigger_users__pgroll_new_description BEFORE INSERT OR UPDATE ON users FOR EACH ROW EXECUTE FUNCTION _pgroll_trigger_users__pgroll_new_description>
    _pgroll_trigger_users_description BEFORE INSERT OR UPDATE ON users FOR EACH ROW EXECUTE FUNCTION _pgroll_trigger_users_description()
```

The `_pgroll_new_description` column has a `NOT NULL` `CHECK` constraint, but the old `description` column is still nullable.

We'll talk about what the two triggers on the table do later.

For now, let's look at the schemas in the database:

```
\dn
```

You should see something like this:

```
+-----------------------------------------+-------------------+
| Name                                    | Owner             |
+-----------------------------------------+-------------------+
| pgroll                                  | postgres          |
| public                                  | pg_database_owner |
| public_01_create_users_table            | postgres          |
| public_02_user_description_set_nullable | postgres          |
+-----------------------------------------+-------------------+
```

We have two schemas: one corresponding to the old schema, `public_01_create_users_table`, and one for the migration we just started, `public_02_user_description_set_nullable`. Each schema contains one view on the `users` table. Let's look at the view in the first schema:

```
\d+ public_01_create_users_table.users
```

The output should contain something like this:

```sql
 SELECT users.id,
    users.name,
    users.description
   FROM users;
```

and for the second view:

```
\d+ public_02_user_description_set_nullable.users
```

The output should contain something like this:

```sql
 SELECT users.id,
    users.name,
    users._pgroll_new_description AS description
   FROM users;
```

The second view exposes the same three columns as the first, but its `description` field is mapped to the `_pgroll_new_description` field in the underlying table. 

By choosing to access the `users` table through either the `public_01_create_users_table.users` or `public_02_user_description_set_nullable.users` view, applications have a choice of which version of the schema they want to see; either the old version without the `NOT NULL` constraint on the `description` field or the new version with the constraint.

When we looked at the schema of the `users` table, we saw that `pgroll` has created two triggers:

```
_pgroll_trigger_users__pgroll_new_description BEFORE INSERT OR UPDATE ON users FOR EACH ROW EXECUTE FUNCTION _pgroll_trigger_users__pgroll_new_description>
_pgroll_trigger_users_description BEFORE INSERT OR UPDATE ON users FOR EACH ROW EXECUTE FUNCTION _pgroll_trigger_users_description()
```

These triggers are used by `pgroll` to ensure that any values written into the old `description` column are copied over to the `_pgroll_new_description` column (rewriting values using the `up` SQL from the migration) and to copy values written to the `_pgroll_new_description` column back into the old `description` column (rewriting values using the`down` SQL from the migration).

Let's see the first of those triggers in action. 

First set the [search path](https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH) for your Postgres session to use the old schema:

```sql
SET search_path = 'public_01_create_users_table'
```

Now insert some data into the `users` table through the `users` view:

```sql
INSERT INTO users(name, description) VALUES ('Alice', 'this is Alice'), ('Bob', NULL)
```

This inserts two new users into the `users` table, one with a `description` and one without.

Let's check that the data was inserted:

```sql
SELECT * FROM users WHERE name = 'Alice' or name = 'Bob'
```

Running this query should show:

```
+--------+-------+---------------------+
| id     | name  | description         |
+--------+-------+---------------------+
| 100001 | Alice | this is Alice       |
| 100002 | Bob   | NULL                |
+--------+-------+---------------------+
```

The trigger should have copied the data that was just written into the old `description` column (without the `NOT NULL` constraint) into the `_pgroll_new_description` column (with the `NOT NULL` constraint )using the `up` SQL from the migration. 

Let's check. Set the search path to the new version of the schema:

```sql
SET search_path = 'public_02_user_description_set_nullable'
```

and find the users we just inserted:

```sql
SELECT * FROM users WHERE name = 'Alice' or name = 'Bob'
```

The output should look like this:

```
+--------+-------+---------------------+
| id     | name  | description         |
+--------+-------+---------------------+
| 100001 | Alice | this is Alice       |
| 100002 | Bob   | description for Bob |
+--------+-------+---------------------+
```

Notice that the trigger installed by `pgroll` has rewritten the `NULL` value inserted into the old schema by using the `up` SQL from the migration definition.

### Completing the migration

Once the old version of the database schema is no longer required (perhaps the old applications that depend on the old schema are no longer in production) the current migration can be completed:

```
pgroll complete
```

After the migration has completed, the old version of the schema is no longer present in the database:

```
\dn
```

shows something like:

```
+-----------------------------------------+-------------------+
| Name                                    | Owner             |
+-----------------------------------------+-------------------+
| pgroll                                  | postgres          |
| public                                  | pg_database_owner |
| public_02_user_description_set_nullable | postgres          |
+-----------------------------------------+-------------------+
```

Only the new version schema `public_02_user_description_set_nullable` remains in the database.

Let's look at the schema of the `users` table to see what's changed there:

```
DESCRIBE users
```

shows something like:

```
+-------------+------------------------+-----------------------------------------------------------------+----------+--------------+-------------+
| Column      | Type                   | Modifiers                                                       | Storage  | Stats target | Description |
+-------------+------------------------+-----------------------------------------------------------------+----------+--------------+-------------+
| id          | integer                |  not null default nextval('_pgroll_new_users_id_seq'::regclass) | plain    | <null>       | <null>      |
| name        | character varying(255) |  not null                                                       | extended | <null>       | <null>      |
| description | text                   |  not null                                                       | extended | <null>       | <null>      |
+-------------+------------------------+-----------------------------------------------------------------+----------+--------------+-------------+
Indexes:
    "_pgroll_new_users_pkey" PRIMARY KEY, btree (id)
    "_pgroll_new_users_name_key" UNIQUE CONSTRAINT, btree (name)
```

The extra `_pgroll_new_description` has been renamed to `description` and the old `description` column has been removed. The column is now marked as `NOT NULL`.

`pgroll` has allowed us to safely roll out this change to the `description` column.

### Summary

We've seen:

* how to apply a couple of `pgroll` migrations to a database.
* how `pgroll` separates migrations into `start` and `complete` phases.
* how data is backfilled to meet constraints at the beginning of the `start` phase.
* that during the `start` phase, `pgroll` uses multiple schema to present different versions of an underlying table to client applications.
* that data written into the old schema version is copied over into the new schema, and vice-versa.
* that completing a migration removes the old schema version and cleans up the underlying table, putting it in its final state.

## Command line reference

The `pgroll` CLI offers the following subcommands:
* [init](#init)
* [start](#start)
* [complete](#complete)
* [rollback](#rollback)
* [status](#status)

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
* [Drop column](#drop-column)
* [Drop constraint](#drop-constraint)
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
    "default": "new default value",
    "up": "SQL expression",
    "down": "SQL expression"
  }
}
```

Example **change default** migrations:

* [35_alter_column_multiple.json](../examples/35_alter_column_multiple.json)

#### Change comment

A change comment operation changes the comment on a column.

**change comment** operations have this structure:

```json
{
  "alter_column": {
    "table": "table name",
    "column": "column name",
    "comment": "new comment for column" or null,
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

Example **create index** migrations:

* [10_create_index.json](../examples/10_create_index.json)
* [37_create_partial_index.json](../examples/37_create_partial_index.json)
* [38_create_hash_index_with_fillfactor.json](../examples/38_create_hash_index_with_fillfactor.json)

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

A drop constraint operation drops a constraint from an existing table.

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
