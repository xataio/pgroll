# Getting started


## Commands

## Migration file

``` json title="initial.json"
{
  "name": "initial",
  "operations:" [
    {
      "create_table": {
        "name": "users",
        "columns": [
          {
            "name": "id",
            "type": "integer",
            "pk": true /* (1)! */
          },
          {
            "name": "name",
            "type": "varchar(255)",
            "unique": true
          }
        ]
      }
    }
  ]
}
```

1.  Look ma, less line noise!
