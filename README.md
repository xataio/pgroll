<div align="center">
  <img src="brand-kit/banner/pgroll-banner-github@2x.png" alt="pgroll logo" />
</div>

<p align="center">
  <a href="https://github.com/xataio/pgroll/blob/main/LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-green" alt="License - Apache 2.0"></a>&nbsp;
  <a href="https://github.com/xataio/pgroll/actions?query=branch%3Amain"><img src="https://github.com/xataio/pgroll/actions/workflows/build.yml/badge.svg" alt="CI Build"></a> &nbsp;
  <a href="https://github.com/xataio/pgroll/releases"><img src="https://img.shields.io/github/release/xataio/pgroll.svg?label=Release" alt="Release"></a> &nbsp;
  <a href="https://xata.io/discord"><img src="https://img.shields.io/discord/996791218879086662?label=Discord" alt="Discord"></a> &nbsp;
  <a href="https://twitter.com/xata"><img src="https://img.shields.io/twitter/follow/xata?style=flat" alt="X (formerly Twitter) Follow" /> </a>
</p>

# pgroll - Zero-downtime, reversible, schema migrations for Postgres

`pgroll` is an open source command-line tool that offers safe and reversible schema migrations for PostgreSQL by serving multiple schema versions simultaneously. It takes care of the complex migration operations to ensure that client applications continue working while the database schema is being updated. This includes ensuring changes are applied without locking the database, and that both old and new schema versions work simultaneously (even when breaking changes are being made!). This removes risks related to schema migrations, and greatly simplifies client application rollout, also allowing for instant rollbacks.

See the [introductory blog post](https://xata.io/blog/pgroll-schema-migrations-postgres) for more about the problems solved by `pgroll`.

## Features

- Zero-downtime migrations (no database locking, no breaking changes).
- Keep old and new schema versions working simultaneously.
- Automatic columns backfilling when needed.
- Instant rollback in case of issues during migration.
- Works against existing schemas, no need to start from scratch.
- Works with Postgres 14.0 or later.
- Works with any Postgres service (including RDS and Aurora).
- Written in Go, cross-platform single binary with no external dependencies.

## How pgroll works

`pgroll` works by creating virtual schemas by using views on top of the physical tables. This allows for performing all the necessary changes needed for a migration without affecting the existing clients.

![Multiple schema versions with pgroll](docs/img/migration-flow@2x.png)


`pgroll` follows a [expand/contract workflow](https://openpracticelibrary.com/practice/expand-and-contract-pattern/). On migration start, it will perform all the additive changes (create tables, add columns, etc) in the physical schema, without breaking it.

When a breaking change is required on a column, it will create a new column in the physical schema, and backfill it from the old column. Also, configure triggers to make sure all writes to the old/new column get propagated to its counterpart during the whole active migration period. The new column will be then exposed in the new version of the schema.

Once the start phase is complete, the new schema version is ready, mapping all the views to the proper tables & columns. Client applications can then access the new schema version, while the old one is still available. This is the moment to start rolling out the new version of the client application.

![Multiple schema versions with pgroll](docs/img/migration-schemas@2x.png?c=0)

When no more client applications are using the old schema version, the migration can be completed. This will remove the old schema, and the new one will be the only one available. No longer needed tables & columns will be removed (no client is using this at this point), and the new ones will be renamed to their final names. Client applications still work during this phase, as the views are still mapping to the proper tables & columns.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Documentation](#documentation)
- [Benchmarks](#benchmarks)
- [Contributing](#contributing)
- [License](#license)
- [Support](#support)

## Installation

### Binaries

Binaries are available for Linux, macOS & Windows, check our [Releases](https://github.com/xataio/pgroll/releases).

### From source

To install `pgroll` from the source, run the following command:

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

## Usage

Follow these steps to perform your first schema migration using `pgroll`:

### Prepare the database

`pgroll` needs to store some internal state in the database. A table is created to track the current schema version and store version history. To prepare the database, run the following command:

```sh
pgroll init --postgres-url postgres://user:password@host:port/dbname
```

### Start a migration

Create a migration file. You can check the [examples](examples) folder for some examples. For instance, use this migration file to create a new `customers` table:

<details>
  <summary>initial_migration.json</summary>

```json
{
  "name": "initial_migration",
  "operations": [
    {
      "create_table": {
        "name": "customers",
        "columns": [
          {
            "name": "id",
            "type": "integer",
            "pk": true
          },
          {
            "name": "name",
            "type": "varchar(255)",
            "unique": true
          },
          {
            "name": "bio",
            "type": "text",
            "nullable": true
          }
        ]
      }
    }
  ]
}
```
</details>

Then run the following command to start the migration:

```sh
pgroll --postgres-url postgres://user:password@host:port/dbname start initial_migration.json
```

This will create a new schema version in the database, and apply the migration operations (create a table). After this command finishes, both the old version of the schema (with no customers table) and the new one (with the customers table) will be accessible simultaneously.

### Configure client applications

After starting a migration, client applications can start using the new schema version. In order to do so, they need to be configured to access it. This can be done by setting the `search_path` to the new schema version name (provided by `pgroll start` output), for instance:

```sql
SET search_path TO 'public_initial_migration';
```

### Complete the migration

Once there are no more client applications using the old schema version, the migration can be completed. This will remove the old schema. To complete the migration, run the following command:

```sh
pgroll --postgres-url postgres://user:password@host:port/dbname complete
```

### Rolling back a migration

At any point during a migration, it can be rolled back to the previous version. This will remove the new schema and leave the old one as it was before the migration started. To rollback a migration, run the following command:

```sh
pgroll --postgres-url postgres://user:password@host:port/dbname rollback
```

## Documentation

For more advanced usage, a tutorial, and detailed options refer to the guides and references in the [Documentation](docs/).

## Benchmarks

Some performance benchmarks are run on each commit to `main` in order to track performance over time. Each benchmark is run against Postgres 14.8, 15.3, 16.4, 17.0 and "latest". Each line on the chart represents the number of rows the benchmark was run against, currently 10k, 100k and 300k rows.

* `Backfill:` Rows/s to backfill a text column with the value `placeholder`. We use our default batching strategy of 10k rows per batch with no backoff.
* `WriteAmplification/NoTrigger:` Baseline rows/s when writing data to a table without a `pgroll` trigger.
* `WriteAmplification/WithTrigger:` Rows/s when writing data to a table when a `pgroll` trigger has been set up.
* `ReadSchema:` Checks the number of executions per second of the `read_schema` function which is a core function executed frequently during migrations.

They can be seen [here](https://xataio.github.io/pgroll/benchmarks.html).

## Contributing

We welcome contributions from the community! If you'd like to contribute to `pgroll`, please follow these guidelines:

* Create an [issue](https://github.com/xataio/pgroll/issues) for any questions, bug reports, or feature requests.
* Check the documentation and [existing issues](https://github.com/xataio/pgroll/issues) before opening a new issue.

### Contributing Code

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Make your changes and write tests if applicable.
4. Ensure your code passes linting and tests.
5. Submit a pull request.

For this project, we pledge to act and interact in ways that contribute to an open, welcoming, diverse, inclusive, and healthy community.

## References

This is a list of projects and articles that helped as inspiration, or otherwise are similar to `pgroll`:

* [Reshape](https://github.com/fabianlindfors/reshape) by Fabian Lindfors
* [PgHaMigrations](https://github.com/braintree/pg_ha_migrations)
* [PostgreSQL at Scale: Database Schema Changes Without Downtime](https://medium.com/paypal-tech/postgresql-at-scale-database-schema-changes-without-downtime-20d3749ed680)
* [Zero downtime schema migrations in highly available databases](http://essay.utwente.nl/92098/1/vanKampen_MA_EEMCS.pdf)
* [Expand and contract pattern](https://openpracticelibrary.com/practice/expand-and-contract-pattern/)

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

If you have any questions, encounter issues, or need assistance, open an issue in this repository our join our [Discord](https://xata.io/discord), and our community will be happy to help.


<br>
<p align="right">Made with :heart: by <a href="https://xata.io">Xata 🦋</a></p>
