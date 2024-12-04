# Set replica identity

## Structure

A set replica identity operation sets the replica identity for a table.

**set replica identity** operations have this structure:

```json
{
  "set_replica_identity": {
    "table": "name of the table",
    "identity": {
      "type": "full | default | nothing | index"
      "index": "name of the index, if type is 'index'"
    }
  }
}
```

:warning: A **set replica identity** operation is applied directly to the underlying table on migration start. This means that both versions of the table exposed in the old and new version schemas will have the new replica identity set. :warning:

## Examples

- [29_set_replica_identity.json](../../examples/29_set_replica_identity.json)
