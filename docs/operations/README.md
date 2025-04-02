# Operations reference

`pgroll` migrations are specified as YAML or JSON files. All migrations follow the same basic structure:

YAML migration:

```yaml
name: "0x_migration_name"
operations: [...]
```

JSON migration:

```json
{
  "name": "0x_migration_name",
  "operations": [...]
}
```

