[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Linux Build](https://github.com/xataio/pg-roll/actions/workflows/build.yml/badge.svg)](https://github.com/xataio/pg-roll/actions?query=branch%3Amain)
[![Release](https://img.shields.io/github/release/xataio/pg-roll.svg?label=Release)](https://github.com/xataio/pg-roll/releases)

# pg-roll - Zero-downtime schema migrations for Postgres

`pg-roll` is an open source command-line tool that radically simplifies schema migrations in Postgres. It takes care of the complex migration operations to ensure that client applications continue working while the database schema is being updated. This includes ensuring changes are applied without locking the database, and that both old and new schema versions work simultaneously (even when breaking changes are being made!). This removes risks related to schema migrations, and greatly simplifies client application rollout, also allowing for instant rollbacks.

## How pg-roll works

TODO

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

To install `pg-roll` from source, run the following command (requires Go 1.21 or later):

```sh
go install github.com/xataio/pg-roll
```

## Usage

Follow these steps to perform your first schema migration using `pg-roll`:

### Prepare the database

`pg-roll` needs to store some internal state in the database. A table is created to track the current schema version and store versions history. To prepare the database, run the following command:

```sh
pg-roll init postgres://user:password@host:port/dbname
```

### Start a migration

Create a migration file, check the [examples](examples) folder for some examples. For instance, use this migration file to create a new `customers` table:

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

This can also be done by setting the `search_path` in the connection string, for instance:

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

* Fork the repository.
* Create a new branch for your feature or bug fix.
* Make your changes and write tests if applicable.
* Ensure your code passes linting and tests.
* Submit a pull request.
[TODO]:* Please see our Contribution Guidelines for more details.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

If you have any questions, encounter issues, or need assistance, please feel free to open an issue on this repository, and our community will be happy to help.


<br>
<p align="right">Made with :heart: by <a href="https://xata.io">Xata</a></p>
