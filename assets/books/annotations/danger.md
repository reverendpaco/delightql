# Danger Gates {.dqlh}

Certain behaviors are safe in most contexts but dangerous in others.
Rather than forbid them outright, delightql gates
them behind *danger URIs*: named safety boundaries that are closed by
default and opened explicitly per-query.

## Syntax {.dqlh}

A danger gate is a `danger://` URI inside annotation delimiters:

```delightql
genre_2024(*) as x (~~danger://semantics/min_multiplicity~~)
  |;| genre_2025(*) as y,
  x.* = y.*
```

The annotation attaches at a continuation point (after a relation). The URI
identifies the specific danger. The annotation takes the URI alone: writing
it opens that gate for this query. State words such as `ON` belong only to
the CLI's `--danger hierarchy=STATE` flag (see Session Baseline).

A state word inside the annotation is an error:

```{.delightql .bad}
// INVALID: the annotation takes no state word
genre_2024(*) as x (~~danger://semantics/min_multiplicity ON~~)
  |;| genre_2025(*) as y,
  x.* = y.*
```

## Scoping {.dqlh}

A danger gate opens for one query and auto-closes at query end. It
does not leak into subsequent queries:

```delightql
// gate is open for this query: 23 rows, Jazz once
genre_2024(*) as x (~~danger://semantics/min_multiplicity~~)
  |;| genre_2025(*) as y,
  x.* = y.*

// gate is closed again: 47 rows, every matching row of both arms
genre_2024(*) as x |;| genre_2025(*) as y,
  x.* = y.*
```

Multiple gates may be opened for the same query:

```delightql
genre_2024(*) as x
  (~~danger://semantics/min_multiplicity~~)
  (~~danger://cardinality/cartesian~~)
  |;| genre_2025(*) as y,
  x.* = y.*
```

A `min_multiplicity` gate must be spent: written on a statement where no
correlated union uses it, it refuses (`semantic/setop/min_multiplicity/unspent`),
and over a correlation that compares only some of the columns the arms share
it refuses too (`semantic/setop/min_multiplicity/partial`) — correlate the
whole row (`x.* = y.*`).

An unaliased self-join is not a danger to acknowledge: two live scopes
sharing a name always refuse (`semantic/scope/duplicate`). Alias one side.

## Session Baseline {.dqlh}

The program starts with every danger OFF. A client to the program, lik the CLI,
can shift the baseline for *guardrail* dangers -- those that control execution
policy (resource limits, safety checks) rather than language semantics:

```bash
dql query --danger delightql-danger://cardinality/cartesian=ON --db test.db "..."
```

Dangers that change *language semantics* (what operators mean) cannot
be overridden from the CLI. They must appear in the source text --
either as inline per-query annotations.

Per-query annotations override the session baseline.
At query end, the danger reverts to the enclosing scope:



## Danger URI Reference {.dqlh}

The full hierarchy of danger URIs, their defaults, and their semantics
is documented in the **Danger URI Taxonomy** appendix. The known
dangers are:

| URI | What it gates |
|-----|---------------|
| `delightql-danger://cardinality/cartesian` | Cross joins without explicit conditions (declared, not yet enforced: such a join runs with the gate closed) |
| `delightql-danger://termination/unbounded` | Recursive CTEs without termination conditions (declared, not yet enforced) |
| `delightql-danger://semantics/min_multiplicity` | A correlated union pairs duplicate copies by minimum multiplicity (SQL's `INTERSECT ALL`) instead of keeping every matching row of both arms; inline only |
