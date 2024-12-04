# Create index

## Structure

A create index operation creates a new index on a set of columns.

**create index** operations have this structure:

```json
{
  "create_index": {
    "table": "name of table on which to define the index",
    "name": "index name",
    "columns": [ "names of columns on which to define the index" ]
    "predicate": "conditional expression for defining a partial index",
    "method": "btree"
  }
}
```

The field `method` can be `btree`, `hash`, `gist`, `spgist`, `gin`, `brin`.
You can also specify storage parameters for the index in `storage_parameters`.
To create a unique index set `unique` to `true`.

## Examples

- [10_create_index.json](../../examples/10_create_index.json)
- [37_create_partial_index.json](../../examples/37_create_partial_index.json)
- [38_create_hash_index_with_fillfactor.json](../../examples/38_create_hash_index_with_fillfactor.json)
- [42_create_unique_index.json](../../examples/42_create_unique_index.json)
