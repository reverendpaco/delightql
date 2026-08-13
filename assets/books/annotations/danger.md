# Danger Gates {.dqlh}

Certain behaviors are safe in most contexts but dangerous in others.
Rather than forbid them outright, delightql gates
them behind *danger URIs*: named safety boundaries that are closed by
default and opened explicitly per-query.

## Syntax {.dqlh}

A danger gate is a `danger://` URI inside annotation delimiters:

```delightql
employee(*) as e (~~danger://cardinality/nulljoin ON~~),
  department(*) as d,
  e.DepartmentId = d.DepartmentId
```

The annotation attaches at a continuation point (after a relation). The URI
identifies the specific danger. The toggle controls it:

| Toggle | Meaning |
|--------|---------|
| `ON` | Enable the dangerous behavior for this query |
| `OFF` | Restore the safe default (useful to override a CLI baseline) |
| `1`--`9` | Graduated severity levels for host-defined behavior |


A bare form without a toggle is an error:

```{.delightql .bad}
// INVALID: no toggle
(~~danger://cardinality/nulljoin~~)
```

## Scoping {.dqlh}

A danger gate opens for one query and auto-closes at query end. It
does not leak into subsequent queries:

```delightql
-- gate is open for this query
employee(*) as e (~~danger://cardinality/nulljoin ON~~),
  department(*) as d,
  e.DepartmentId = d.DepartmentId

-- gate is closed again -- safe defaults restored
employee(*) as e, department(*) as d,
  e.DepartmentId = d.DepartmentId
```

Multiple gates may be opened for the same query:

```delightql
employee(*) as e
  (~~danger://cardinality/nulljoin ON~~)
  (~~danger://cardinality/cartesian ON~~),
  department(*) as d
```

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
is documented in the **Danger URI Taxonomy** appendix. The initial
dangers are:

| URI | What it gates |
|-----|---------------|
| `delightql-danger://cardinality/nulljoin` | NULL-matching joins (`=` compiles to `IS NOT DISTINCT FROM` in join position) |
| `delightql-danger://cardinality/cartesian` | Cross joins without explicit conditions |
| `delightql-danger://termination/unbounded` | Recursive CTEs without termination conditions |
