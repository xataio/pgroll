# Hello Richard!

This guide covers the key concepts of `pgroll` and how to get started with it.

## Concepts

`pgroll` introduces a few concepts that are important to understand before using the tool.

### Migration workflow

`pgroll` migrations are applied in two steps, following an [expand/contract pattern](https://openpracticelibrary.com/practice/expand-and-contract-pattern/).

![migration flow](img/schema-changes-flow@2x.png)

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
