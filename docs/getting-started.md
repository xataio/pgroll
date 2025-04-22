# Getting started

This guide covers the key concepts of `pgroll` and how to get started with it.

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

## Supported Postgres versions

`pgroll` supports Postgres versions >= 14.

:warning: In Postgres 14, row level security policies on tables are not respected by `pgroll`'s versioned views. This is because `pgroll` is unable to create the views with the `(security_invoker = true)` option, as the ability to do so was added in Postgres 15. If you use RLS in Postgres 14 `pgroll` is likely a poor choice of migration tool. All other `pgroll` features are fully supported across all supported Postgres versions.

