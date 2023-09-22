[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-green)](https://github.com/xataio/pg-roll/blob/main/LICENSE)
[![Linux Build](https://github.com/xataio/pg-roll/actions/workflows/build.yml/badge.svg)](https://github.com/xataio/pg-roll/actions?query=branch%3Amain)
[![Release](https://img.shields.io/github/release/xataio/pg-roll.svg?label=Release)](https://github.com/xataio/pg-roll/releases)

# pg-roll - Zero-downtime schema migrations for Postgres

`pg-roll` is an open source command-line tool that offers safe and un-doable schema migrations for PostgreSQL by serving multiple schema versions simultaneously. It takes care of the complex migration operations to ensure that client applications continue working while the database schema is being updated. This includes ensuring changes are applied without locking the database, and that both old and new schema versions work simultaneously (even when breaking changes are being made!). This removes risks related to schema migrations, and greatly simplifies client application rollout, also allowing for instant rollbacks.

## How pg-roll works

`pg-roll` works by creating virtual schemas by using views on top of the physical tables. This allows for performing all the necessary changes needed for a migration without affecting the existing clients.

![Multiple schema versions with pg-roll](docs/img/migration-flow.svg)


`pg-roll` follows a [expand/contract workflow](https://openpracticelibrary.com/practice/expand-and-contract-pattern/). On migration start, it will perform all the additive changes (create tables, add columns, etc) in the physical schema, without breaking it.

When a breaking change is required on a column, it will create a new column in the physical schema, and backfill it from the old column. Also configure triggers to make sure all writes to the old/new column get propagated to its counterpart during the whole active migration period. The new column will be then exposed in the new version of the schema.

Once start phase is complete, the new schema version is created, mapping all the views to the proper tables & columns. Client applications can then access the new schema version, while the old one is still available. This is the moment to start rolling out the new version of the client application.

![Multiple schema versions with pg-roll](docs/img/migration-schemas.svg)

When no more client applications are using the old schema version, the migration can be completed. This will remove the old schema, and the new one will be the only one available. No longer needed tables & columns will be removed (no client is using this at this point), and the new ones will be renamed to their final names. Client applications still work during this phase, as the views are still mapping to the proper tables & columns.

## Features

- Zero-downtime migrations (no database locking, no breaking changes).
- Keep old and new schema versions working simultaneously.
- Automatic columns backfilling when needed.
- Instant rollback in case of issues during migration.
- Works against existing schemas, no need to start from scratch.
- Works with Postgres 14.0 or later.
- Written in Go, cross-platform single binary with no external dependencies.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)
- [Support](#support)

## Installation

### Binaries

Binaries are available for Linux, macOS & Windows, check our [Releases](releases).

### From source

To install `pg-roll` from source, run the following command:

```sh
go install github.com/xataio/pg-roll
```

Note: requires [Go 1.21](https://golang.org/doc/install) or later.

## Usage

Follow these steps to perform your first schema migration using `pg-roll`:

### Prepare the database

`pg-roll` needs to store some internal state in the database. A table is created to track the current schema version and store versions history. To prepare the database, run the following command:

```sh
pg-roll init postgres://user:password@host:port/dbname
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
pg-roll --postgres-url postgres://user:password@host:port/dbname start initial_migration.json
```

This will create a new schema version in the database, and apply the migration operations (create a table). After this command finishes, both the old version of the schema (with no customers table) and the new one (with the customers table) will be accessible simultaneously.

### Configure client applications

After starting a migration, client applications can start using the new schema version. In order to do so, they need to be configured to access it. This can be done by setting the `search_path` to the new schema version name (provided by `pg-roll start` output), for instance:

```sql
SET search_path TO 'public_initial_migration';
```

This can also be done by setting the `currentSchema` in the connection string, for instance:

```sh
postgres://user:password@host:port/dbname?currentSchema=public_initial_migration
```

### Complete the migration

Once there are no more client applications using the old schema version, the migration can be completed. This will remove the old schema. To complete the migration, run the following command:

```sh
pg-roll --postgres-url postgres://user:password@host:port/dbname complete
```

### Rolling back a migration

At any point during a migration, it can be rolled back to the previous version. This will remove the new schema and leave the old one as it was before the migration started. To rollback a migration, run the following command:

```sh
pg-roll --postgres-url postgres://user:password@host:port/dbname rollback
```

### Advanced Usage

For more advanced usage and detailed options, refer to the [Documentation](docs).

## Contributing

We welcome contributions from the community! If you'd like to contribute to `pg-roll`, please follow these guidelines:

* Create an [issue](https://github.com/xataio/pg-roll/issues) for any questions, bug reports, or feature requests.
* Check the documentation and [existing issues](https://github.com/xataio/pg-roll/issues) before opening a new issue.

### Contributing Code

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Make your changes and write tests if applicable.
4. Ensure your code passes linting and tests.
5. Submit a pull request.

For this project, we pledge to act and interact in ways that contribute to an open, welcoming, diverse, inclusive, and healthy community.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

If you have any questions, encounter issues, or need assistance, open an issue in this repository, and our community will be happy to help.


<br>
<p align="right">Made with :heart: by <a href="https://xata.io">Xata</a></p>
