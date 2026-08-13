# Appendix: Danger URI Taxonomy {.appendix .dqlh}

Certain behaviors are safe in most contexts but dangerous in others.
Rather than forbid them outright, delightql gates them behind *danger
URIs* -- named safety boundaries that are closed by default and opened
explicitly per-query.

```delightql
-- open a specific danger for one query
employee(*) as e (~~danger://cardinality/nulljoin ON~~),
  department(*) as d,
  e.DepartmentId = d.DepartmentId

-- the danger auto-closes at query end
employee(*) as e, department(*) as d,
  e.DepartmentId = d.DepartmentId
-- this query uses safe defaults again
```

The URI is a stable identifier. It doubles as the canonical reference
for documentation, tooling, and diagnostics -- the same role that
error URIs serve for compilation errors.

## Design Principles {.dqlh}

1. **Off by default.** Every danger starts OFF. The safe behavior is
   active unless the programmer explicitly requests otherwise.

2. **Domain first.** The top level identifies the language domain:
   `dql/`, `ddl/`, `dml/`. This mirrors the error URI hierarchy.

3. **What-goes-wrong second.** The second level names the category of
   harm: `cardinality/` (row-count blowup), `termination/`
   (non-halting computation), `precision/` (silent data loss). Where
   error URIs use *what phase failed* (parse, semantic), danger URIs
   use *what goes wrong* -- because dangers are not phase-specific.

4. **Prefix matching does the work.** Each level narrows usefully.
   `danger://dql` catches any DQL danger.
   `danger://cardinality` catches any cardinality blowup.
   `danger://cardinality/nulljoin` catches only that specific case.

5. **No bare form.** `(~~danger://cardinality/nulljoin~~)` without
   `ON` or `OFF` is an error. Being explicit about the toggle is the
   entire point.

6. **Query-scoped.** A danger gate opens for one query and auto-closes
   at query end. It does not leak into subsequent queries.

7. **The URI is the documentation.** The danger URI in source code is
   also the canonical reference for what the danger means and why it
   exists.


## Syntax {.dqlh}

```delightql
employee(*) as e (~~danger://cardinality/nulljoin ON~~),
  department(*) as d,
  e.DepartmentId = d.DepartmentId
```

The annotation lives inside the annotation delimiters `(~~ ... ~~)` and
attaches at a continuation point (after a relation). It is an
annotation that travels with the query but is not part of the
relational algebra.

| Component | Meaning |
|-----------|---------|
| `danger://` | URI scheme identifying a danger gate |
| `delightql-danger://cardinality/nulljoin` | Hierarchical path to the specific danger |
| `ON` | Enable the dangerous behavior for this query |
| `OFF` | Restore the safe default (useful to override a CLI baseline) |
| `ALLOW` | Permit but do not force -- the compiler may use the dangerous path if needed |
| `1`--`9` | Graduated severity levels for host-defined behavior |

### Toggle Values {.dqlh}

`ON` and `OFF` are the common cases. They are binary: the dangerous
behavior is either active or not.

`ALLOW` is a middle ground. It tells the compiler that the dangerous
behavior is acceptable but not required. The compiler may choose the
safe path when it can and the dangerous path when it must. This is
useful for queries where the programmer has verified that the data
does not trigger the danger but wants the compiler to retain latitude.

The severity levels `1` through `9` exist for host-defined policies
where binary on/off is too coarse. The language defines no semantics
for specific levels -- the host interprets them. Example uses:

- A linter that warns at level 3 but errors at level 7
- A monitoring system that logs at level 1 but alerts at level 5
- A deployment pipeline that permits level 1-4 in staging but only
  level 1-2 in production

The severity levels are ordered: higher numbers indicate greater
willingness to accept the danger. A tool checking "is danger level
at least N?" can compare numerically.

Multiple dangers may be opened for the same query:

```delightql
employee(*) as e
  (~~danger://cardinality/nulljoin ON~~)
  (~~danger://cardinality/cartesian ON~~),
  department(*) as d,
  e.DepartmentId = d.DepartmentId
```

## Defaults and Overrides {.dqlh}

The program starts with a default table where every danger is OFF:

```
danger://cardinality/nulljoin            OFF
danger://cardinality/cartesian           OFF
danger://termination/unbounded           OFF
danger://semantics/min_multiplicity      OFF
```

### Override Scopes {.dqlh}

Not all dangers accept overrides from the same places. The scope at
which a danger can be overridden depends on whether it changes
*language semantics* or *execution guardrails*:

| URI | Inline | File | CLI | Category |
|-----|:------:|:----:|:---:|----------|
| `delightql-danger://cardinality/nulljoin` | yes | yes | **no** | semantic |
| `delightql-danger://cardinality/cartesian` | yes | yes | yes | guardrail |
| `delightql-danger://termination/unbounded` | yes | yes | yes | guardrail |
| `delightql-danger://semantics/min_multiplicity` | yes | yes | **no** | semantic |

**Semantic dangers** change what operators *mean*. The `nulljoin`
gate redefines `=` in join position from SQL `=` to
`IS NOT DISTINCT FROM`. A DQL script should mean the same thing
regardless of who runs it and what CLI flags they pass. Semantic
overrides must live in the source text -- either inline on the
query or at the top of the file -- so the script is self-documenting.

**Guardrail dangers** control whether the engine *permits* certain
operations. They do not change expression semantics. Cartesian
product rejection and unbounded recursion prevention are resource
limits, not language redefinitions. These may be overridden at any
scope, including the CLI.

The guiding principle: **operator semantics are fixed by the source
text.** CLI flags may change SQL *shape* (via `config://`) or
*execution policy* (via guardrail `danger://`), but never *language
meaning*.

### Session Baseline (CLI) {.dqlh}

The CLI can shift the baseline for guardrail dangers:

```bash
dql query --danger delightql-danger://cardinality/cartesian=ON --db test.db "..."
```

Attempting to override a semantic danger from the CLI is an error:

```{.bash .bad}
# REJECTED: nulljoin is a semantic danger -- use inline annotation
dql query --danger delightql-danger://cardinality/nulljoin=ON --db test.db "..."
```

### Override Precedence {.dqlh}

Per-query annotations override the file-level directive. The
file-level directive overrides the session baseline. At query end,
the danger reverts to the file-level or session-level value:

```
CLI baseline  ---->  file directive  ---->  per-query  ---->  revert
    OFF                   ON                   OFF             ON
```

## Prefix Matching {.dqlh}

Danger hooks match by prefix, identically to error hooks:

| Expected | Matches |
|----------|---------|
| `danger://dql` | any DQL danger |
| `danger://cardinality` | `nulljoin`, `cartesian`, any future cardinality danger |
| `danger://cardinality/nulljoin` | null-join only |
| `danger://termination` | `unbounded`, any future termination danger |


## URI Hierarchy {.dqlh}

### `delightql-danger://cardinality/` --- Row-Count Blowups {.dqlh}

The query may produce far more rows than the programmer expects.
These dangers guard against silent multiplicative explosions in
result cardinality.

| URI | Default | Condition | What happens when ON |
|-----|---------|-----------|---------------------|
| `delightql-danger://cardinality/nulljoin` | OFF | `=` in join position compiles to SQL `=` | `=` in join position compiles to `IS NOT DISTINCT FROM`. NULL keys match each other, producing a cartesian product of all NULL rows. |
| `delightql-danger://cardinality/cartesian` | OFF | Cross joins without an explicit condition are rejected | Cross joins without conditions are permitted. |

**Why `nulljoin` is a cardinality danger.** The NULL-by-NULL cross
product is a multiplicative blowup. Five NULLs on the left and three
on the right produce fifteen matched rows. The danger is not that NULLs
participate in the join -- it is that they participate *combinatorially*.

**Why `cartesian` is a cardinality danger.** A cross join of two
million-row tables produces a trillion rows. Explicit cross joins are
sometimes intended (for generating combinations), but an *accidental*
cross join -- one caused by a missing join condition -- is one of the
most common and costly SQL mistakes.


### `delightql-danger://termination/` --- Non-Halting Computation {.dqlh}

The query may not terminate.

| URI | Default | Condition | What happens when ON |
|-----|---------|-----------|---------------------|
| `delightql-danger://termination/unbounded` | OFF | Recursive CTEs must include a termination condition | Recursive CTEs without termination conditions are permitted. |

**Why `unbounded` is a termination danger.** A recursive CTE without a
termination condition produces an infinite result. In practice, the
database engine will hit a resource limit and error -- but only after
consuming significant time and memory. The compiler can detect the
absence of a termination condition statically and reject it early.


### `delightql-danger://semantics/` --- Operator Semantics {.dqlh}

The query's meaning changes. These dangers alter what an operator
computes, not merely whether it is permitted. They are semantic
dangers: inline-only, never CLI-overridable.

| URI | Default | Condition | What happens when ON |
|-----|---------|-----------|---------------------|
| `delightql-danger://semantics/min_multiplicity` | OFF | Intersection-via-correlation uses bidirectional semijoin (UNION ALL of EXISTS-filtered operands), producing m+n copies of matching tuples | Intersection-via-correlation uses ROW_NUMBER + equi-join, producing min(m,n) copies -- true INTERSECT ALL multiplicity. |

**Why `min_multiplicity` is a semantic danger.** The bidirectional
semijoin and the ROW_NUMBER path compute different multisets for
duplicate tuples. Three copies in the left operand and two in the
right yield five rows under bidirectional semijoin but two under
min-multiplicity. The difference only surfaces with genuinely
duplicate tuples, but it changes what the operator *means* --
the same query produces different results. This is a semantic
redefinition, so it must live in the source text.


### Future Categories {.dqlh}

The hierarchy is designed to grow. Possible future categories:

#### `dql/precision/` --- Silent Data Loss {.dqlh}

| URI | Condition |
|-----|-----------|
| `dql/precision/implicit_cast` | Implicit type coercion that loses information |
| `dql/precision/truncation` | String or numeric truncation without warning |

#### `dml/destructive/` --- Irreversible Mutations {.dqlh}

| URI | Condition |
|-----|-----------|
| `dml/destructive/unfiltered_update` | UPDATE without a WHERE condition |
| `dml/destructive/unfiltered_delete` | DELETE without a WHERE condition |


## Relationship to Error URIs {.dqlh}

Danger URIs and error URIs are sibling systems:

| | Error URIs | Danger URIs |
|-|-----------|-------------|
| **Scheme** | `error://` | `danger://` |
| **When** | After compilation fails | Before compilation (gate check) |
| **Mechanism** | Prefix matching for error hooks | Prefix matching for gate control |
| **Top level** | Domain (`dql/`, `ddl/`, `dml/`) | Domain (`dql/`, `ddl/`, `dml/`) |
| **Second level** | Phase (`parse/`, `semantic/`) | What goes wrong (`cardinality/`, `termination/`) |
| **Default** | Errors always fire | Dangers always off |

Both use hierarchical URIs. Both support prefix matching. Both serve
as stable identifiers for documentation and tooling. The difference is
directional: error URIs *report* what went wrong; danger URIs *prevent*
what could go wrong.
