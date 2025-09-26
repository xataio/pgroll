# Tutorial

This section will walk you through applying your first migrations using `pgroll`.

We'll apply two migrations to a fresh database and have a look at what `pgroll` does under the hood.

Prerequisites:

* `pgroll` installed and accessible somewhere on your `$PATH`
* A fresh Postgres instance against which to run migrations

A good way to get a throw-away Postgres instance for use in the tutorial is to use [Docker](https://www.docker.com/). Start a Postgres instance in Docker with:

```
docker run --rm --name for-pgroll -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:18
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

<YamlJsonTabs>
```yaml
operations:
  - create_table:
      name: users
      columns:
        - name: id
          type: serial
          pk: true
        - name: name
          type: varchar(255)
          unique: true
        - name: description
          type: text
          nullable: true
```
```json
{
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
</YamlJsonTabs>

Take this file and save it as `sql/01_create_users_table.yaml`.

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
pgroll start sql/01_create_users_table.yaml --complete
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

<YamlJsonTabs>
```yaml
operations:
  - alter_column:
      table: users
      column: description
      nullable: false
      up: "SELECT CASE WHEN description IS NULL THEN 'description for ' || name ELSE description END"
      down: description
```
```json
{
  "operations": [
    {
      "alter_column": {
        "table": "users",
        "column": "description",
        "nullable": false,
        "up": "SELECT CASE WHEN description IS NULL THEN 'description for ' || name ELSE description END",
        "down": "description"
      }
    }
  ]
}
```
</YamlJsonTabs>

Save this migration as `sql/02_user_description_set_nullable.yaml` and start the migration:

```
pgroll start 02_user_description_set_nullable.yaml
```

After some progress updates you should see a message saying that the migration has been started successfully.

<details>
  <summary>What's happening behind the progress updates?</summary>

  In order to add the new `description` column, `pgroll` creates a temporary `_pgroll_new_description` column and copies over the data from the existing `description` column, using the `up` SQL from the migration. As we have 10^5 rows in our table, this process takes some time. This process is called _backfilling_ and it is performed in batches to avoid locking all rows in the table simultaneously.

  The `_pgroll_needs_backfill` column in the `users` table is used to track which rows have been backfilled and which have not. This column is set to `true` for all rows at the start of the migration and set to `false` once the row has been backfilled. This ensures that rows inserted or updated while the backfill is in process aren't backfilled twice.
</details>

At this point it's useful to look at the table data and schema to see what `pgroll` has done. Let's look at the data first:

```sql
SELECT * FROM users ORDER BY id LIMIT 10
```

You should see something like this:
```
+----+---------+------------------------+-------------------------+------------------------+
| id | name    | description            | _pgroll_new_description | _pgroll_needs_backfill |
|----+---------+------------------------+-------------------------+------------------------|
| 1  | user_1  | <null>                 | description for user_1  | False                  |
| 2  | user_2  | description for user_2 | description for user_2  | False                  |
| 3  | user_3  | <null>                 | description for user_3  | False                  |
| 4  | user_4  | <null>                 | description for user_4  | False                  |
| 5  | user_5  | <null>                 | description for user_5  | False                  |
| 6  | user_6  | description for user_6 | description for user_6  | False                  |
| 7  | user_7  | <null>                 | description for user_7  | False                  |
| 8  | user_8  | description for user_8 | description for user_8  | False                  |
| 9  | user_9  | <null>                 | description for user_9  | False                  |
| 10 | user_10 | <null>                 | description for user_10 | False                  |
+----+---------+------------------------+-------------------------+------------------------+
```

This is the "expand" phase of the [expand/contract pattern](https://openpracticelibrary.com/practice/expand-and-contract-pattern/) in action; `pgroll` has added a `_pgroll_new_description` field to the table and populated the field for all rows using the `up` SQL from the `02_user_description_set_nullable.yaml` file:

<YamlJsonTabs>
```yaml
up: "SELECT CASE WHEN description IS NULL THEN 'description for ' || name ELSE description END"
```
```json
"up": "SELECT CASE WHEN description IS NULL THEN 'description for ' || name ELSE description END",
```
</YamlJsonTabs>

This has copied over all `description` values into the `_pgroll_new_description` field, rewriting any `NULL` values using the provided SQL.

Now let's look at the table schema:

```sql
DESCRIBE users
```

You should see something like this:

```
+-------------------------+------------------------+-----------------------------------------------------+
| Column                  | Type                   | Modifiers                                           |
|-------------------------+------------------------+-----------------------------------------------------|
| id                      | integer                |  not null default nextval('users_id_seq'::regclass) |
| name                    | character varying(255) |  not null                                           |
| description             | text                   |                                                     |
| _pgroll_new_description | text                   |                                                     |
| _pgroll_needs_backfill  | boolean                |  default true                                       |
+-------------------------+------------------------+-----------------------------------------------------+
Indexes:
    "users_pkey" PRIMARY KEY, btree (id)
    "users_name_key" UNIQUE CONSTRAINT, btree (name)
Check constraints:
    "_pgroll_check_not_null_description" CHECK (_pgroll_new_description IS NOT NULL) NOT VALID
Triggers:
    _pgroll_trigger_users__pgroll_new_description BEFORE INSERT OR UPDATE ON users FOR EACH ROW EXECUTE FUNCTION _pgroll_trigger_users__pgroll_new_description()
    _pgroll_trigger_users_description BEFORE INSERT OR UPDATE ON users FOR EACH ROW EXECUTE FUNCTION _pgroll_trigger_users_description()
```

The `_pgroll_new_description` column has a `NOT NULL` `CHECK` constraint, but the old `description` column is still nullable.

<details>
  <summary>Why is the <code>NOT NULL</code> constraint on the new <code>_pgroll_new_description</code> column <code>NOT VALID</code>?</summary>

   Defining the constraint as `NOT VALID` means that the `users` table will not be scanned to enforce the `NOT NULL` constraint for existing rows. This means the constraint can be added quickly without locking rows in the table. `pgroll` assumes that the `up` SQL provided by the user will ensure that no `NULL` values are written to the `_pgroll_new_description` column.
</details>

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

<details>
  <summary>How do applications configure which version of the schema to use</summary>

  `pgroll` allows old and new versions of an application to exist side-by-side during a migration. Each version of the application should be configured with the name of the correct version schema, so that the application sees the database schema that it expects.

  This is done by setting the Postgres **search_path** for the client's session and is described in more detail in the **Client applications** section below.
</details>

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

A few things have happened:

- The extra `_pgroll_new_description` has been renamed to `description`.
- The `_pgroll_needs_backfill` column has been removed.
- The old `description` column has been removed.
- The `description` column is now marked as `NOT NULL`.
- The triggers to copy data back and forth between the old and new column have been removed.

**At this point, the migration is complete**. There is just one version schema in the database: `public_02_user_description_set_nullable` and the underlying `users` table has the expected schema.

<details>
  <summary>How is the column made <code>NOT NULL</code> without locking?</summary>

  Because there is an existing `NOT NULL` constraint on the column, created when the migration was started, making the column `NOT NULL` when the migration is completed does not require a full table scan. See the Postgres [docs](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-DESC-SET-DROP-NOT-NULL) for `SET NOT NULL`.
</details>

### Rollbacks

The expand/contract approach to migrations means that the old version of the database schema (`01_create_users_table` in this example) remains operational throughout the migration. This has two key benefits:

- Old versions of client applications that rely on the old schema continue to work.
- Rollbacks become trivial!

Looking at the second of these items, rollbacks, let's see how to roll back a `pgroll` migration. We can start another migration now that our last one is complete:

<YamlJsonTabs>
```yaml
operations:
  - add_column:
      table: users
      column:
        name: is_atcive
        type: boolean
        nullable: true
        default: "true"
```
```json
{
  "operations": [
    {
      "add_column": {
        "table": "users",
        "column": {
          "name": "is_atcive",
          "type": "boolean",
          "nullable": true,
          "default": "true"
        }
      }
    }
  ]
}
```
</YamlJsonTabs>

Create this migration and save it as `sql/03_add_is_active_column.yaml`.

(the misspelling of `is_active` is intentional!)

This migration adds a new column to the `users` table. As before, we can start the migration with this command:

```
pgroll start 03_add_is_active_column.yaml
```

Once again, this creates a new version of the schema:

```
\dn
```

Shows something like:

```
+-----------------------------------------+-------------------+
| Name                                    | Owner             |
|-----------------------------------------+-------------------|
| pgroll                                  | postgres          |
| public                                  | pg_database_owner |
| public_02_user_description_set_nullable | postgres          |
| public_03_add_is_active_column          | postgres          |
+-----------------------------------------+-------------------+
```

And adds a new column with a temporary name to the `users` table:

```
+-----------------------+------------------------+-----------------------------------------------------------------+----------+--------------+-------------+
| Column                | Type                   | Modifiers                                                       | Storage  | Stats target | Description |
|-----------------------+------------------------+-----------------------------------------------------------------+----------+--------------+-------------|
| id                    | integer                |  not null default nextval('_pgroll_new_users_id_seq'::regclass) | plain    | <null>       | <null>      |
| name                  | character varying(255) |  not null                                                       | extended | <null>       | <null>      |
| description           | text                   |  not null                                                       | extended | <null>       | <null>      |
| _pgroll_new_is_atcive | boolean                |  default true                                                   | plain    | <null>       | <null>      |
+-----------------------+------------------------+-----------------------------------------------------------------+----------+--------------+-------------+
```

The new column is not present in the view in the old version of the schema:

```
\d+ public_02_user_description_set_nullable.users
```

Shows:

```
 SELECT users.id,
    users.name,
    users.description
   FROM users;
```

But is exposed by the new version.

```
\d+ public_03_add_is_active_column.user
```

Shows:

```
 SELECT users.id,
    users.name,
    users.description,
    users._pgroll_new_is_atcive AS is_atcive
   FROM users;
```

However, there's a typo in the column name: `isAtcive` instead of `isActive`. The migration needs to be rolled back:

```
pgroll rollback
```

The rollback has removed the old version of the schema:

```
+-----------------------------------------+-------------------+
| Name                                    | Owner             |
|-----------------------------------------+-------------------|
| pgroll                                  | postgres          |
| public                                  | pg_database_owner |
| public_02_user_description_set_nullable | postgres          |
+-----------------------------------------+-------------------+
```

And the new column has been removed from the underlying table:

```
+-------------+------------------------+-----------------------------------------------------------------+----------+--------------+-------------+
| Column      | Type                   | Modifiers                                                       | Storage  | Stats target | Description |
|-------------+------------------------+-----------------------------------------------------------------+----------+--------------+-------------|
| id          | integer                |  not null default nextval('_pgroll_new_users_id_seq'::regclass) | plain    | <null>       | <null>      |
| name        | character varying(255) |  not null                                                       | extended | <null>       | <null>      |
| description | text                   |  not null                                                       | extended | <null>       | <null>      |
+-------------+------------------------+-----------------------------------------------------------------+----------+--------------+-------------+
```

Since the original schema version, `02_user_description_set_nullable`, was never removed, existing client applications remain unaware of the migration and subsequent rollback.

### Client applications

`pgroll` uses the [expand/contract pattern](https://openpracticelibrary.com/practice/expand-and-contract-pattern/) to roll out schema changes. Each migration creates a new version schema in the database.

In order to work with the multiple versioned schema that `pgroll` creates, clients need to be configured to work with one of them.

This is done by having client applications configure the [search path](https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH) when they connect to the Postgres database.

For example, this fragment for a Go client application shows how to set the `search_path` after a connection is established:

```go
db, err := sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
if err != nil {
    return nil, err
}

searchPath := "02_user_description_set_nullable"
_, err = db.Exec(fmt.Sprintf("SET search_path = %s", pq.QuoteIdentifier(searchPath)))
if err != nil {
    return nil, fmt.Errorf("failed to set search path: %s", err)
}
```

In practice, the `searchPath` variable would be provided to the application as an environment variable.

### Summary

We've seen:

* how to apply a couple of `pgroll` migrations to a database.
* how `pgroll` separates migrations into `start` and `complete` phases.
* how data is backfilled to meet constraints at the beginning of the `start` phase.
* that during the `start` phase, `pgroll` uses multiple schema to present different versions of an underlying table to client applications.
* that data written into the old schema version is copied over into the new schema, and vice-versa.
* that completing a migration removes the old schema version and cleans up the underlying table, putting it in its final state.
* that rollbacks are safe and easy, thank to the expand/contract pattern.
