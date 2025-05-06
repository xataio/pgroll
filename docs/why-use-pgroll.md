# Why use pgroll?

[pgroll](https://pgroll.com/) is a schema migration tool for Postgres. It is designed for application developers working on applications that require frequent schema changes but also need to maintain zero downtime around those schema changes. `pgroll` takes a different approach compared to most other migration tools on the market.

There are two aspects that characterize `pgroll`'s approach to migrations.

## Multi-version migrations

Making a schema change with `pgroll` results in two versions of the schema; the one before the change and the one after the change - this allows applications to select which version of the schema they want to work with and allows side-by-side rollout of applications that require the new schema changes with old applications that may be incompatible with it.

## Lock-safe migrations

Migrations using `pgroll` are expressed declaratively, rather than using SQL directly. This allows `pgroll` to implement the steps required to perform the schema change in a safe manner, ensuring that any locks required on the affected objects are held for the shortest possible time.

If you want to avoid worrying about schema changes, install `pgroll` and create your next migration with us.
