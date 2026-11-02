# Iciclecreek.Azure.Storage.OData

Evaluates Azure Table Storage OData `$filter` expressions against `TableEntity` rows, in process.

Azure Table Storage filters use the OData v3 literal syntax that `TableClient.CreateQueryFilter` generates —
`datetime'…'`, `guid'…'`, `X'…'`/`binary'…'` and `123L` for 64-bit integers — which OData v4 parsers reject.
This package is the filter evaluator shared by the Iciclecreek.Azure.Storage providers, for any code that
needs to apply a table filter itself.

```csharp
using Iciclecreek.Azure.Storage.OData;

Func<TableEntity, bool> predicate = ODataFilterParser.Parse("PartitionKey eq 'p1' and Age ge 21");
var matches = entities.Where(predicate);
```

Supports `and`, `or`, `not`, parentheses, and `eq`/`ne`/`gt`/`ge`/`lt`/`le` comparisons against
`PartitionKey`, `RowKey`, `Timestamp` and any entity property.
