# pg-roll

:warning: Under development :warning:

PostgreSQL zero-downtime migrations made easy.

## Getting started (development)

* Bring a development PostgreSQL up:

```sh
docker compose up
```

* Start a migration:

```sh
go run . start examples/01_create_tables.json
```

* Inspect the results:

```sh
psql postgres://localhost -U postgres
```

```sql
\d+ public.*
\d+ 01_create_tables.*
```

* Complete the migration:

```sh
go run . complete examples/01_create_tables.json
```