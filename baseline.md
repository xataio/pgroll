# [WIP] Baseline

The problem of *baselining* arises when a database already exists with some schema objects, but `pgroll` needs to begin managing it from a specific point without reapplying or duplicating existing changes. 

This  occurs when adopting `pgroll` for an existing production database .The challenge lies in determining the correct "baseline version" from which migrations should apply, ensuring that the `pgroll` doesn't attempt to re-run previous migrations, while still guaranteeing that future changes are tracked and applied correctly. 

This document proposes an implementation of baselining for `pgroll` .

## Proposal

Add a new `pgroll baseline <version_name>` command which states effectively: “call the current state of my database `<version_name>` and allow me to run `pgroll migrations` from this point onwards”.

Running:

```bash
$ pgroll baseline 01_initial_version
```

creates a new ‘baseline version’ entry in the `pgroll.migrations` table having a new `migration_type` field of `baseline` . This special `baseline` migration is stored with an empty `migration` :

```yaml
{"name": "01_initial_version", "operations": []}
```

And the `resulting_schema` field is populated with the database schema at the time the `pgroll baseline` command was run.

A migration YAML file corresponding to the baseline version is expected to be created, by hand or using a tool like `pgdump` , and committed to the `migrations/` directory for the project:

```yaml
name: 01_initial_version
operations:
  - sql:
      up: |
      CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)
      CREATE FUNCTION ...
```

Thus the `migrations/` directory retains the full set of migrations necessary to bootstrap a new database to the desired schema state.

In most respects, `pgroll` then treats the `baseline` migration as a no-op migration. It is ignored by the `pgroll pull` and `pgroll migrate` commands and running `pgroll baseline` does not create a version schema for the migration.

The remainder of the document describes how users interact with the new `pgroll baseline` command in more detail.

## Workflows

### Workflow 1 - Creating a baseline and subsequent migrations

1. The user has a database with existing schema and they want to start using `pgroll` for managing migrations for it. The user runs:

```bash
# 01_initial_migration is the name the user wishes to
# give to the initial version of the database

$ pgroll baseline 01_initial_migration
```

1. (optional, but recommended) Create a `01_initial_migration.yaml` migration file that brings the target database up to the initial version. 
2. Add the first `pgroll` migration for the next schema change as `02_add_column.yaml`:

```yaml
operations:
  - add_column:
      table: products
      up: UPPER(name)
      column:
        name: description
        type: varchar(255)
        nullable: true
```

1. The user runs `pgroll migrate` to start the new (`02_add_column`) migration:

```yaml
$ pgroll migrate 
```

`pgroll migrate` does not attempt to apply the `01_initial_migration.yaml` migration because it is recorded as the baseline version in the `pgroll.migrations` table.

1. From this point on, `pgroll` migrations can be written and applied in the usual manner.

### Workflow 2 - Pulling migrations with a baseline in the schema history

1. The user is working with a database that has an `01_initial_migration` baseline as the first migration in the history. The following migrations have been subsequently applied to the database:
    - `02_create_some_table`
    - `03_add_constraint`
2. The user runs:

```bash
$ pgroll pull migrations/
```

To pull the schema history from the database into a local `migrations` dir. The following migrations are pulled from the target database.

```bash
$ ls migrations/
02_create_some_table.yaml
03_add_constraint.yaml
```

The baseline migration `01_initial_migration` is not pulled as it is a placeholder in the schema history and the real migration is expected to have been created locally and pushed to the `migrations/` directory in source control, ie already be present under `migrations/`.

## Implementation milestones

- Implement a (hidden) `pgroll baseline` command that creates a new `baseline` type migration in `pgroll.migrations`.
- Ensure that the `start` , `complete`, `rollback` workflow works when the parent migration is a `baseline` migration.
- Update `pull` command to not pull anything before and including the most recent `baseline` migration.
- Update the `migrate` command to not try to apply `baseline`  migrations.
- Implement the ability to have multiple baselines in a migration history.

## FAQ

- **What happens if a user runs `pgroll baseline` multiple times?**
    - Yes, this is allowed and allows users to ‘roll up’ previous changes into a new baseline migration. Only migrations after the most recent `baseline` version are considered by the `pgroll pull` and `pgroll migrate` commands.
- **Do baseline migrations need to be marked as such locally on the filesystem?**
    - They shouldn’t be marked as a baseline locally because a baseline migration in one database will be applied as a regular migration migration to another. The property of ‘being a baseline migration’ is subjective and differs between the same database in different environments.
- **Does `baseline` need to be run before any other migrations or does it make sense to be able to run it when other migrations have already been run?**
    - `pgroll baseline` can be run multiple times; migrations that come before the most recent baseline version are ignored by `migrate` and `pull.`