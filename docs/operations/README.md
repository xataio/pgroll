# Operations reference

`pgroll` migrations are specified as YAML or JSON files. All migrations follow the same basic structure:

YAML migration:

```yaml
version_schema: <version schema name>
operations: [...]
```

JSON migration:

```json
{
  "version_schema": "<version schema name>",
  "operations": [...]
}
```

The `version_schema` field is optional.

## Migration names vs version schema names

When a `pgroll` migration is run a version schema for the migration is created. The name of the version schema defaults to the name of the migration file (minus any `.yaml`, or .`json` suffix). For example, this migration:

```yaml
operations:
  - create_table:
      name: items
      columns:
        - name: id
          type: serial
          pk: true
        - name: name
          type: varchar(255)
```

will create a version schema called `01_create_table`, assuming the migration filename is `01_create_table.yaml`.

The name of the version schema that a migration will create when run can be overridden using the `version_schema` field:

```yaml
version_schema: my_version_schema
operations:
  - create_table:
      name: items
      columns:
        - name: id
          type: serial
          pk: true
        - name: name
          type: varchar(255)
```

This migration will create a version schema called `my_version_schema` regardless of the migration filename.
