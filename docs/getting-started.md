# Welcome to pgroll's documentation

`pgroll` is a migration tool for PostgreSQL that makes it easy to apply schema changes safely, without application downtime. It supports instant rollbacks and helps ensure applications stay available during database schema changes by allowing applications to access different versions of your database schema.

## Getting started

Learn how `pgroll` simplifies zero-downtime migrations for you

* [Why use pgroll](why-use-pgroll)
* [Installation](installation)
* [Concepts](concepts)
* [Write your first migration](tutorial)

## Guides

Learn how to use `pgroll` in your developer workflow

* [Integrate pgroll into your project](guides/clientapps)
* [Writing up and down migrations](guides/updown)

## Connect with us

If you have questions reach out to us on our
* [Discord server](https://xata.io/discord)
* [Github Discussion board](https://github.com/xataio/pgroll/discussions)

If you want to report issues or submit feedback visit
* [Github Issues](https://github.com/xataio/pgroll/issues)

## Supported Postgres versions

`pgroll` supports Postgres versions >= 14.

:warning: In Postgres 14, row level security policies on tables are not respected by `pgroll`'s versioned views. This is because `pgroll` is unable to create the views with the `(security_invoker = true)` option, as the ability to do so was added in Postgres 15. If you use RLS in Postgres 14 `pgroll` is likely a poor choice of migration tool. All other `pgroll` features are fully supported across all supported Postgres versions.

