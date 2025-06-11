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

The `version_schema` field is optional and specifies the name of the schema that will be created for the migration. If not specified, the name of the migration file (minus any `.yaml` or `.json` suffix) will be used as the schema name.
