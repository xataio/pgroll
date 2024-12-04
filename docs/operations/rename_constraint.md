# Rename constraint

## Structure

A rename constraint operation renames a constraint.

**rename constraint** operations have this structure:

```json
{
  "rename_constraint": {
    "table": "table name",
    "from": "old constraint name",
    "to": "new constraint name"
  }
}
```

## Examples

- [33_rename_constraint.json](../../examples/33_rename_constraint.json)
