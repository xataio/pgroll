# Command line reference

The `pgroll` CLI has the following top-level flags:

- `--postgres-url`: The URL of the postgres instance against which migrations will be run.
- `--schema`: The Postgres schema in which migrations will be run (default `"public"`).
- `--pgroll-schema`: The Postgres schema in which `pgroll` will store its internal state (default: `"pgroll"`). One `--pgroll-schema` may be used safely with multiple `--schema`s.
- `--lock-timeout`: The Postgres `lock_timeout` value to use for all `pgroll` DDL operations, specified in milliseconds (default `500`).
- `--role`: The Postgres role to use for all `pgroll` DDL operations (default: `""`, which doesn't set any role).

Each of these flags can also be set via an environment variable:

- `PGROLL_PG_URL`
- `PGROLL_SCHEMA`
- `PGROLL_STATE_SCHEMA`
- `PGROLL_LOCK_TIMEOUT`
- `PGROLL_ROLE`

The CLI flag takes precedence if a flag is set via both an environment variable and a CLI flag.
