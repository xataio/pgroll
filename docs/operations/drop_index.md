# Drop index

A drop index operation drops an index from a table.

**drop index** operations have this structure:

```json
{
  "drop_index": {
    "name": "name of index to drop"
  }
}
```

## Example **drop index** migrations:

- [11_drop_index.json](../../examples/11_drop_index.json)