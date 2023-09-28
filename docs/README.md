# :books: `pgroll` Documentation

* [Concepts](#concepts)
    * [Migration workflow](#migration-workflow)
    * [Multiple schema versions](#multiple-schema-versions)
    * [Client applications](#client-applications)
* [Installation](#installation)
* [Tutorial](#tutorial)
* [Command line reference](#command-line-reference)
* [Operations reference](#operations-reference)

:warning: This section is under development.

## Concepts

### Migration workflow

### Multiple schema versions

### Client applications

In order to work with the multiple versioned schema that `pgroll` creates, clients need to be configured to work with one of them. 

This is done by having client applications configure the [search path](https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH) when they connnect to the Postgres database.

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
  "name": "02_user_description_not_null",
  "operations": [
    {
      "alter_column": {
        "table": "users",
        "column": "description",
        "not_null": true,
        "up": "(SELECT CASE WHEN description IS NULL THEN 'description for ' || name ELSE description END)",
        "down": "description"
      }
    }
  ]
}
```

Save this migration as `sql/02_user_description_not_null.json` and start the migration:

```
pgroll start 02_user_description_not_null.json
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

`pgroll` has added a `_pgroll_new_description` field to the table and populated the field for all rows using the `up` SQL from the `02_user_description_not_null.json` file:

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
+-------------------------------------+-------------------+
| Name                                | Owner             |
+-------------------------------------+-------------------+
| pgroll                              | postgres          |
| public                              | pg_database_owner |
| public_01_create_users_table        | postgres          |
| public_02_user_description_not_null | postgres          |
+-------------------------------------+-------------------+
```

We have two schemas: one corresponding to the old schema, `public_01_create_users_table`, and one for the migration we just started, `public_02_user_description_not_null`. Each schema contains one view on the `users` table. Let's look at the view in the first schema:

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
\d+ public_02_user_description_not_null.users
```

The output should contain something like this:

```sql
 SELECT users.id,
    users.name,
    users._pgroll_new_description AS description
   FROM users;
```

The second view exposes the same three columns as the first, but its `description` field is mapped to the `_pgroll_new_description` field in the underlying table. 

By choosing to access the `users` table through either the `public_01_create_users_table.users` or `public_02_user_description_not_null.users` view, applications have a choice of which version of the schema they want to see; either the old version without the `NOT NULL` constraint on the `description` field or the new version with the constraint.

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
SET search_path = 'public_02_user_description_not_null'
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
+-------------------------------------+-------------------+
| Name                                | Owner             |
+-------------------------------------+-------------------+
| pgroll                              | postgres          |
| public                              | pg_database_owner |
| public_02_user_description_not_null | postgres          |
+-------------------------------------+-------------------+
```

Only the new version schema `public_02_user_description_not_null` remains in the database.

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
* [Create index](#create-index)
* [Create table](#create-table)
* [Drop column](#drop-column)
* [Drop constraint](#drop-constraint)
* [Drop index](#drop-index)
* [Drop table](#drop-table)
* [Raw SQL](#raw-sql)
* [Rename table](#rename-table)
* [Alter column](#alter-column)
    * [Rename column](#rename-column)
    * [Change type](#change-type)
    * [Add check constraint](#set-check)
    * [Add foreign key](#set-fk)
    * [Add not null constraint](#set-notnull)
    * [Add unique constraint](#set-unique)

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
      } 
    }
  }
}
```

Example **add column** migrations:

* [03_add_column.json](../examples/03_add_column.json)
* [06_add_column_to_sql_table.json](../examples/06_add_column_to_sql_table.json)
* [17_add_rating_column.json](../examples/17_add_rating_column.json)
* [26_add_column_with_check_constraint.json](../examples/26_add_column_with_check_constraint.json)

### Alter column
### Create index

A create index operation creates a new btree index on a set of columns.

**create index** operations have this structure:

```json
{
  "create_index": {
    "table": "name of table on which to define the index",
    "name": "index name",
    "columns": [ "names of columns on which to define the index" ]
  }
}
```

Example **create index** migrations:

* [10_create_index.json](../examples/10_create_index.json)

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
  }
},
```

Example **create table** migrations:

* [01_create_tables.json](../examples/01_create_tables.json)
* [02_create_another_table.json](../examples/02_create_another_table.json)
* [08_create_fruits_table.json](../examples/08_create_fruits_table.json)
* [12_create_employees_table.json](../examples/12_create_employees_table.json)
* [14_add_reviews_table.json](../examples/14_add_reviews_table.json)
* [19_create_orders_table.json](../examples/19_create_orders_table.json)
* [20_create_posts_table.json](../examples/20_create_posts_table.json)
* [25_add_table_with_check_constraint.json](../examples/25_add_table_with_check_constraint.json)

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

Example **drop column** migrations:

* [09_drop_column.json](../examples/09_drop_column.json)

### Drop constraint
### Drop index
### Drop table
### Raw SQL
### Alter column

#### Rename column
#### Change type
#### Add check constraint
#### Add foreign key
#### Add not null constraint
#### Add unique constraint
