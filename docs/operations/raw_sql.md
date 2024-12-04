# Raw SQL

## Structure

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

## Examples

- [05_sql.json](../../examples/05_sql.json)
- [32_sql_on_complete.json](../../examples/32_sql_on_complete.json)
