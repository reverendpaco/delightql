# Meta-ize Operator {.dqlh}

The meta-ize operator reifies a relation's schema as a relation -- each
column becomes a row. Where `*`{.delightql .sigil} inside a functor
returns all data rows, `^`{.delightql .sigil} returns all columns as
rows of metadata.

## Schema as Relation (`^`) {.dqlh}

```delightql
users(^)
```

This returns one row per column in `users`:

| colname | colposition | coltype |
|---------|-------------|---------|
| id      | 1           | INTEGER |
| first_name | 2        | TEXT    |
| last_name | 3         | TEXT    |
| age     | 4           | INTEGER |
| email   | 5           | TEXT    |

: Output of `users(^)`

The `^`{.delightql .sigil} operator belongs to the unary continuation operator family
-- unary operators that transform table access:

| Operator  | Meaning                                      |
|-----------|----------------------------------------------|
| `*`       | Qualify column names (data access)           |
| `.*`      | Unqualified columns (natural join candidate) |
| `.(cols)` | USING semantics on specific columns          |
| `^`       | Column metadata as rows                      |

: Table continuation operators

These operators compose freely: `users(*.(id))` means "qualified + USING on id."


## Postfix Form {.dqlh}

`users(^)`{.delightql} is sugar for `users() ^`{.delightql}. The
postfix form works on any relational expression, not just base tables:

```delightql
-- schema of a projection (2 rows)
users(*) |> (first_name, age) ^

-- schema of a join
users(*), products(*) ^

-- schema of an aggregation
users(*) |> %(country ~> count:(*) as n) ^
```

The postfix `^`{.delightql .sigil} applies to the entire expression
to its left, returning its schema as a relation.

## Composability {.dqlh}

Because `^`{.delightql .sigil} produces a regular relation, all DQL
operations apply -- filtering, projection, pipes, joins, and set
operators:

```delightql
-- text columns only
users(^), coltype = "TEXT" |> (colname)
```

```delightql
-- columns shared between two year-partitioned tables
users_2024(^) |;| users_2023(^), x.* = y.*
```


> The output of `^` is itself a relation with a fixed schema
> (scope, column_name, ordinal). Applying `^` to a `^` result
> would return the schema of the metadata relation -- three rows
> describing scope, column_name, ordinal themselves.
