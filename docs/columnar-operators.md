# Columnar operators

Operators transform one Arrow batch into zero or more Arrow batches. `BuiltinOp`
provides four; `ColumnarProcessor` lets you write your own.

## The processor interface

```java
@FunctionalInterface
public interface ColumnarProcessor {
    void process(ColumnarContext context, VectorSchemaRoot batch);
}
```

`ColumnarContext` has one public method, `forward(VectorSchemaRoot)`. Call it once per
output batch. Calling it zero times drops the batch; calling it several times fans a
batch out into several downstream batches.

## Built-in operators

`BuiltinOp` implements `ColumnarProcessor` and always forwards exactly one batch.
Every factory takes the `BufferAllocator` that will own the output.

### filter

```java
var large = BuiltinOp.filter(allocator, (batch, row) ->
        ((BigIntVector) batch.getVector("amount")).get(row) > 4);
```

`RowPredicate.test(VectorSchemaRoot, int)` is called once per row. Rows that pass are
copied into a new batch with the same schema, in their original order. Metadata columns
travel with the rows, so `__offset` still identifies the source record after filtering.

Read vectors by name and cast to the concrete Arrow vector type. Check
`vector.isNull(row)` before reading a nullable column, because the typed `get`
accessors do not do it for you.

### select

```java
var projected = BuiltinOp.select(allocator, "user", "amount");
```

Keeps the named payload columns, in the order given, then appends whichever reserved
metadata columns exist in the input. Duplicate names are ignored after the first. A
name that is not present throws `ColumnarException("Arrow column does not exist: sku")`.

Selecting one payload column from a decoded batch therefore yields five columns: the
one you asked for plus `__key`, `__timestamp`, `__partition`, and `__offset`. You do not
have to list metadata columns to keep them, and you cannot drop them with `select`.

### withColumns

```java
var doubled = BuiltinOp.withColumns(
        allocator,
        new DerivedColumn(
                new Field("double_amount", FieldType.nullable(new ArrowType.Int(64, true)), null),
                (batch, row) -> ((BigIntVector) batch.getVector("amount")).get(row) * 2));
```

Each `DerivedColumn` pairs an Arrow `Field` with a `RowValue`, a function from
`(batch, row)` to an `Object`. A derived column whose name matches an existing column
**replaces** it in place, keeping its position; a new name is appended after the
existing columns. Reserved names are rejected at construction time, not at run time.

Returned values are coerced to the declared Arrow type:

| Declared vector                        | Accepted value                                         |
| -------------------------------------- | ------------------------------------------------------ |
| `VarChar` (`Utf8`)                     | anything; `toString()` is applied and encoded as UTF-8 |
| `VarBinary` (`Binary`)                 | `byte[]` or `ByteBuffer`                               |
| `BigInt`, `Int`, `SmallInt`, `TinyInt` | integral `Number`, checked for overflow                |
| `UInt1`, `UInt2`, `UInt4`, `UInt8`     | non-negative integral `Number`, checked for overflow   |
| `Float4`, `Float8`                     | any `Number`                                           |
| `Bit` (`Bool`)                         | `Boolean`                                              |
| dates and timestamps                   | epoch `Number` or matching `java.time` value           |
| decimal                                | `BigDecimal` or numeric text                           |
| list and fixed-size list               | `Collection<?>`                                        |
| struct and map                         | `Map<?, ?>`                                            |
| sparse and dense union                 | the first member compatible with the value             |

`null` writes a null into the column. Because `Utf8` accepts anything, it is the safe
declaration for a value whose type you cannot pin down.

Dictionary-encoded fields use their physical index vector, so derived values are the
dictionary indexes. A custom processor is still appropriate when it must also mutate
the external dictionary provider.

### groupBy

```java
var totals = BuiltinOp.groupBy(
        allocator,
        List.of("user"),
        new Aggregation("amount", "total", AggregateFunction.SUM),
        new Aggregation("amount", "count", AggregateFunction.COUNT));
```

Groups rows by the values of the key columns and retains those groups across every call
made to the same built topology. The output is the current cumulative result, ordered
by first appearance, with key columns first and aggregate columns after them.

| Function | Output type             | Semantics                                                  |
| -------- | ----------------------- | ---------------------------------------------------------- |
| `COUNT`  | `Int(64)`               | rows in the group, nulls included                          |
| `SUM`    | the input column's type | exact integral accumulation; an all-null group yields null |
| `MIN`    | the input column's type | nulls skipped; values compared with `Comparable`           |
| `MAX`    | the input column's type | nulls skipped; values compared with `Comparable`           |

Pass an `ArrowType` as the fourth `Aggregation` argument to override any output type.
Integral narrowing and accumulation are checked and throw `ArithmeticException` on
overflow. An unknown key or input column throws
`ColumnarException("Arrow column does not exist: ...")`, and an empty key list throws
`groupBy requires at least one key column`.

Two consequences of the output schema are easy to miss:

- Metadata columns are dropped. Unless you group by them, `__key`, `__timestamp`,
  `__partition`, and `__offset` are gone. A downstream `BlobCodec` sink then emits a
  record with timestamp `0`, and a downstream `RowCodec` sink emits records with a null
  key and timestamp `0`. Group by `__partition` or add the columns back with
  `withColumns` if you need them.
- Rebuilding the topology creates fresh aggregate state. Keep one
  `BuiltColumnarTopology` for the lifetime of a running partition or consumer group.

Key values are read as Java objects: `Utf8` columns become `String`, binary columns
become read-only `ByteBuffer`, numeric columns become the boxed numeric type. Grouping
by dates, timestamps, decimals, lists, structs, or unions writes through the same
coercion rules as `withColumns`.

## Custom processors

Anything the built-ins cannot express, write directly:

```java
final class TopN implements ColumnarProcessor {
    private final BufferAllocator allocator;
    private final int limit;

    @Override
    public void process(ColumnarContext context, VectorSchemaRoot batch) {
        var amounts = (BigIntVector) batch.getVector("amount");
        var rows = IntStream.range(0, batch.getRowCount())
                .boxed()
                .sorted(Comparator.comparingLong(amounts::get).reversed())
                .limit(limit)
                .mapToInt(Integer::intValue)
                .toArray();
        context.forward(copyRows(batch, rows, allocator));   // your own copy helper
    }
}

topology.addOperator("top-10", () -> new TopN(allocator, 10), source);
```

The supplier form is the one to use for stateful processors: it is invoked once when
the topology is built, and that instance receives every later batch for the node.

Splitting a batch is just several `forward` calls:

```java
context.forward(smallRows);
context.forward(largeRows);
```

Both batches flow to every downstream node, one after the other.

Dropping a batch is forwarding nothing:

```java
if (batch.getRowCount() == 0) {
    return;     // the input is closed for you
}
```

## Buffer ownership

Arrow buffers are off-heap and reference-counted. Leaks show up as an exception when
the allocator closes, not as pressure on the Java heap. The rules for a columnar
topology are short:

1. The input batch belongs to the framework. Never close it.
2. If you forward the input batch unchanged, the framework keeps it alive and closes it
   after the downstream nodes finish.
3. If you do not forward the input, the framework closes it as soon as `process`
   returns.
4. A batch you create and forward transfers to the framework, which closes it when the
   batch's downstream work is done.
5. A batch you create and do not forward is yours. Close it, ideally with
   try-with-resources.
6. If `process` throws, the framework closes the input, everything already forwarded,
   and every output produced so far for that node, then rethrows.

Outside a topology the rule is simpler and stricter. Whatever a method returns to you,
you close:

```java
try (var allocator = new RootAllocator();
        var batch = codec.decode(records)) {
    var produced = codec.encode(batch);
}
```

`ArrowIpcSerde`'s deserializer, `BatchCodec.decode`, and `RowBridge.rowsToBatch` all
return roots the caller owns. `BatchCodec.encode` returns plain `byte[]`-backed records
and closes its internal temporaries itself.

### The allocator

Create one `RootAllocator` for the application and close it at shutdown:

```java
try (var allocator = new RootAllocator()) {
    // build codecs, operators, and the topology with this allocator
}
```

Closing an allocator that still has outstanding buffers throws and names the leaked
allocations, which makes it an effective leak detector in tests. Pass the same
allocator to every codec and operator in a topology so that accounting is
consistent; child allocators with limits are supported by Arrow but are not required
by anything here.

Remember the JVM flag, `--add-opens=java.base/java.nio=ALL-UNNAMED`, described in
[Configuration](configuration.md#jvm-flags).
