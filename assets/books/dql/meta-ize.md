# Meta-ize Operator {.dqlh}

The meta-ize operator reifies a relation's schema as a relation -- each
column becomes a row. Where `*`{.delightql .sigil} inside a functor
returns all data rows, `^`{.delightql .sigil} returns all columns as
rows of metadata.

## Schema as Relation (`^`) {.dqlh}

```delightql
album(^)
```

This returns one row per column in `album`:

| scope  | column_name   | ordinal  |
| ------ | ------------- | -------- |
| album  | album_id      | 1        |
| album  | title         | 2        |
| album  | artist_id     | 3        |

: Output of `album(^)`

The `^`{.delightql .sigil} operator belongs to the unary continuation operator family
-- unary operators that transform table access:

| Operator  | Meaning                                      |
|-----------|----------------------------------------------|
| `*`       | Qualify column names (data access)           |
| `.*`      | Unqualified columns (natural join candidate) |
| `.(cols)` | USING semantics on specific columns          |
| `^`       | Column metadata as rows                      |

: Table continuation operators

These operators compose freely: `album(*.(artist_id))` means "qualified + USING on artist_id."


## Postfix Form {.dqlh}

`album(^)`{.delightql} is sugar for `album() ^`{.delightql}. The
postfix form works on any relational expression, not just base tables:

```delightql
// schema of a projection (2 rows)
album(*) |> (title, artist_id) ^

// schema of a join
album(*), artist(*.(artist_id)) ^

// schema of an aggregation
album(*) |> %(artist_id ~> count:(*) as n) ^
```

The postfix `^`{.delightql .sigil} applies to the entire expression
to its left, returning its schema as a relation.

## Composability {.dqlh}

Because `^`{.delightql .sigil} produces a regular relation, all DQL
operations apply -- filtering, projection, pipes, joins, and set
operators:

```delightql
// key columns only
track(^), +like(column_name, "%_id") |> (column_name)
```

```delightql
// columns shared between two tables
customer(^) as x |;| employee(^) as y, x.column_name = y.column_name
```


> The output of `^` is itself a relation with a fixed schema
> (scope, column_name, ordinal). Applying `^` to a `^` result
> would return the schema of the metadata relation -- three rows
> describing scope, column_name, ordinal themselves.
