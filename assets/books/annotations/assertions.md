# Assertions {.dqlh}

Assertions verify properties of a relation at a given point in a
pipeline. They are annotations whose body is parsed
as DQL, using interior relation semantics to scope the current
relation.

```delightql
users(*), age > 30
  (~~assert , age > 30 |> forall(*) ~~)
  |> (first_name, email)
```

The assertion above verifies that every row has `age > 30` at that
point in the pipeline. The main pipeline is unaffected -- the
relation after the assertion is the same as before it.

## Assertion Syntax {.dqlh}

An assertion uses the annotation syntax with the reserved name `assert`:

```delightql
(~~assert <continuation> ~~)
```

The body after `assert` is parsed as a DQL continuation -- the same
syntax used inside functor parentheses for interior relations (see
**Interior Relations**). The `(~~assert` delimiter scopes a sub-query
on the current relation. The leading `,`{.delightql .sigil} or
`|>`{.delightql .sigil} is a continuation on the implicit relation,
exactly like `users(, age > 20)`.

The sub-query inside the assertion is a **fork**: it branches from the
main pipeline, evaluates independently, and the main pipeline
continues with the original relation regardless of the assertion's
outcome.

The assertion body is pure DQL. It may terminate with an assertion
view that produces a single-column, single-row boolean relation (see
**Assertion Views** below). If no assertion view is specified,
`exists(*)` is implied -- the assertion passes if at least one row
survives the body's filters:

```delightql
-- these are equivalent
users(*) (~~assert , age > 0 ~~)
users(*) (~~assert , age > 0 |> exists(*) ~~)
```

The bare form is the common case. An explicit view is only needed
for `notexists(*)`, `forall(*)`, or `equals(*)`.

## Named Assertions {.dqlh}

Assertions may carry a name. The name appears after `assert` as a colon-delimited string:

```delightql
(~~assert:"age is positive" , age > 0 |> forall(*) ~~)
(~~assert:"has email" , email != null |> forall(*) ~~)
(~~assert:"at least 3 rows" ~> count:(*) as n, n >= 3 |> exists(*) ~~)
```

The name should be an author-supplied label that serves as the primary key when
recording assertion outcomes. Unnamed assertions still work. They will receive
a synthetic key (derived from source location and body hash) but lose cross-run
trackability.

## Data Assertions {.dqlh}

Data assertions check properties of the rows at a point in the
pipeline. They end with an assertion view that reduces the relation
to a boolean:

```delightql
-- at least one row with age > 20 exists
users(*) (~~assert , age > 20 |> exists(*) ~~)

-- every row has age > 20
users(*) (~~assert , age > 20 |> forall(*) ~~)

-- no nulls in email
users(*) (~~assert , email is null |> notexists(*) ~~)

-- exactly 3 rows
users(*) (~~assert ~> count:(*) as cnt, cnt == 3 |> exists(*) ~~)

-- id is unique (no duplicates)
users(*) (~~assert ~> %(id ~> count:(*) as n), n > 1 |> notexists(*) ~~)

-- age is always positive
users(*) (~~assert , age > 0 |> forall(*) ~~)
```

The assertion body is any valid DQL.

## Schema Assertions {.dqlh}

Schema assertions check structural properties of the relation. They
use the meta-ize operator `^`{.delightql .sigil} (see **Meta-ize
Operator**) to convert the schema to a queryable relation, then apply
standard assertions:

```delightql
-- column "age" exists
users(*) |> (name, age)
  (~~assert ^, colname = "age" |> exists(*) ~~)

-- exactly 3 columns
users(*) |> (a, b, c)
  (~~assert ^ ~> count:(*) as n, n == 3 |> exists(*) ~~)

-- no TEXT columns
users(*)
  (~~assert ^, coltype = "TEXT" |> notexists(*) ~~)
```

For exact schema matching, use `equals(*)`{.delightql} with the
reverse pipe `<|`{.delightql .sigil} (see **Reverse Pipe**) to
compare against an expected schema:

```delightql
users(*)
  (~~assert ^ |> equals(*) <| _(colname, colpos
                                  ------
                                  "age", 1;
                                  "last_name", 2;
                                  "first_name", 3) ~~)
```

## Relational Equality {.dqlh}

The `equals(*)`{.delightql} view checks bag equality between two
relations via the reverse pipe. Bag equality means: same column names
in the same order, and the same bag of rows with duplicates and
multiplicities preserved.

```delightql
-- assert query result matches expected rows
users(*), age > 50
  (~~assert |> equals(*) <| _(first_name, age
                                ------
                                "Alice", 55;
                                "Bob", 62) ~~)
```

The right operand of `<|`{.delightql .sigil} can be any relational
expression -- a CTE, a table access, or an anonymous table literal.

## Assertion Views {.dqlh}

Assertion bodies end with a view from `std::prelude` that reduces a
relation to a single-row, single-column table. The column is named
`bool` and contains `true` or `false`. Every assertion view has this
same output shape -- the runner reads the `bool` column to determine
the verdict.

```
┌───────┐
│ bool  │
├───────┤
│ true  │
└───────┘
```

| View           | Semantics                         | SQL pattern                                    |
|----------------|-----------------------------------|------------------------------------------------|
| `exists(*)`    | At least one row in the input     | `SELECT EXISTS(...)  AS bool`                  |
| `notexists(*)` | No rows in the input              | `SELECT NOT EXISTS(...) AS bool`               |
| `forall(*)`    | All input rows survived filtering | `SELECT NOT EXISTS(... WHERE NOT ...) AS bool` |
| `equals(*)`    | Bag equality of two relations     |                                                |

: Assertion views (auto-imported from std::prelude)

All four views produce the same relation: one row, one column named
`bool`. This uniformity means the assertion mechanism needs no
special dispatch -- the pipeline compiles the body, executes it, and
reads `bool` from the single result row.
