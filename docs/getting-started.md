# Getting started

This guide covers the key concepts of `pgroll` and how to get started with it.

## Supported Postgres versions

`pgroll` supports Postgres versions >= 14.

:warning: In Postgres 14, row level security policies on tables are not respected by `pgroll`'s versioned views. This is because `pgroll` is unable to create the views with the `(security_invoker = true)` option, as the ability to do so was added in Postgres 15. If you use RLS in Postgres 14 `pgroll` is likely a poor choice of migration tool. All other `pgroll` features are fully supported across all supported Postgres versions.

