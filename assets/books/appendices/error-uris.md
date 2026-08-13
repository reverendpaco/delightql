# Appendix: Error URI Taxonomy {.appendix .dqlh}

Every compilation error carries a hierarchical URI that identifies
the error category. Error hooks use these URIs for prefix matching:

```delightql
-- matches any DQL semantic error
users(*) |> (foo.*) (~~error://semantic ~~)

-- matches only table resolution failures
nonexistent_table(*) (~~error://semantic/resolution/table ~~)

-- matches any error at all
bad_query(*) (~~error ~~)
```

The URI is a stable identifier independent of the error message text.
It doubles as the canonical reference for documentation, tooling, and
diagnostics — `dql explain <identifier>` looks any of them up (badge
form, canonical URL, or bare hierarchy) from the compiler's own
registry. The full (badge) form is
`delightql-error://<hierarchy>`, which corresponds mechanically to
`https://delightql.org/uri/error/<hierarchy>`; contexts that already
declare the kind — the `(~~error:// ~~)` hook sigil, machine-record
brackets — carry the bare hierarchy.

## Design Principles {.dqlh}

1. **The scheme is the root.** `delightql-error://` names both the
   project and the kind; the hierarchy under it starts directly at the
   compiler phase. There is no path-level root segment.

2. **Phase first.** The top segments are `parse`, `semantic`, `dml`,
   `operational`, `runtime`, and `target` — parse errors mean the
   source text is structurally invalid; semantic errors mean the
   structure is valid but the meaning is wrong; `target` errors
   originate in a foreign engine (`target/postgres/<class>/<sqlstate>`
   embeds the world's taxonomy as the leaf).

3. **Prefix matching does the work.** Each level narrows usefully:
   `error://semantic` catches any semantic error;
   `error://semantic/resolution` catches any name binding failure;
   `error://target/postgres` catches any Postgres-side failure.

4. **No `validation`.** The term is too vague. `semantic` says what
   the category *is*. `constraint`, `arity`, `resolution` say what
   went *wrong*.

## Prefix Matching {.dqlh}

Error hooks match by prefix. An expected hierarchy of `semantic`
matches any actual URI that starts with `delightql-error://semantic/`:

| Expected | Matches |
|----------|---------|
| `error://dql` | any DQL error (parse or semantic) |
| `error://semantic` | any semantic error |
| `error://semantic/resolution` | `resolution/table`, `resolution/column`, `resolution/ambiguous`, etc. |
| `error://semantic/resolution/table` | table resolution failures only |
| `error://parse` | any parse failure |
| *(bare)* | any error |

## URI Hierarchy {.dqlh}

### `delightql-error://parse/` --- Structural Failures {.dqlh}

The source text does not form a valid CST, or CST-to-AST conversion
finds malformed structure. The problem is syntactic.

| URI | Condition | Trigger |
|-----|-----------|---------|
| `delightql-error://parse` | Any parse failure | |
| `delightql-error://parse/literal` | Malformed literal | `0xGG`, `0o89` |
| `delightql-error://parse/expression` | Malformed expression | `x +`, empty expression |
| `delightql-error://parse/anon` | Malformed anonymous table | `_(a @ 2, 3)` |
| `delightql-error://parse/pipe` | Malformed pipe expression | `x /->` |
| `delightql-error://parse/function` | Malformed function call | missing name, lambda body |
| `delightql-error://parse/case` | Malformed CASE expression | missing arm, missing result |
| `delightql-error://parse/window` | Malformed window spec | invalid frame mode |
| `delightql-error://parse/json_path` | Malformed JSON path | `[name]`, `{42}` |
| `delightql-error://parse/projection` | Empty or invalid projection | `\|> -(*)` |
| `delightql-error://parse/subquery` | Malformed scalar subquery | missing table, missing continuation |
| `delightql-error://parse/pattern` | Malformed pattern literal | invalid `/pattern/` format |

Fine-grained leaves (e.g. `delightql-error://parse/literal/hex`) can be added
later. The second level is the useful grain for error hooks.

### `delightql-error://semantic/` --- Semantic Failures {.dqlh}

The structure is valid but the meaning is wrong. Names do not
resolve, arities do not match, or domain constraints are violated.

#### `delightql-error://semantic/resolution/` --- Name Binding Failures {.dqlh}

| URI | Condition | Trigger |
|-----|-----------|---------|
| `delightql-error://semantic/resolution` | Any name binding failure | |
| `delightql-error://semantic/resolution/table` | Table or view not found | `nonexistent(*)` |
| `delightql-error://semantic/resolution/column` | Column cannot be resolved | `\|> (bad_col)` |
| `delightql-error://semantic/resolution/function` | Function or HO view not found | |
| `delightql-error://semantic/resolution/sigma` | Sigma predicate not found | |
| `delightql-error://semantic/resolution/ambiguous` | Name matches multiple entities | cross-join with shared column |
| `delightql-error://semantic/resolution/scope` | Name exists but unreachable | column behind pipe barrier, post-group leak |

**Why `ambiguous` lives under `resolution`.** Ambiguity is the dual
of not-found: resolution fails because there are zero matches (not
found) or multiple matches (ambiguous). Both are failures of name
binding.

**Why `scope` lives under `resolution`.** The name exists in the
schema, but the current scope cannot see it. The column is behind a
pipe barrier, or a group-by reduced the visible columns. It is a
resolution failure with a specific cause.

#### `delightql-error://semantic/arity/` --- Wrong Argument Count {.dqlh}

| URI | Condition | Trigger |
|-----|-----------|---------|
| `delightql-error://semantic/arity` | Wrong argument count (general) | |
| `delightql-error://semantic/arity/function` | Function call arity | |
| `delightql-error://semantic/arity/predicate` | Predicate arity | `+between(1, age)` |
| `delightql-error://semantic/arity/sigma` | Sigma predicate arity | |
| `delightql-error://semantic/arity/pattern` | Positional pattern element count | `users(a, b, c)` |

**Why `arity` is separate from `resolution`.** Resolution is about
*finding* the entity. Arity is about *calling* it. A function can
resolve successfully and still fail on arity. These are different
failure modes with different fixes: "did you spell it right?" vs
"did you pass the right number of arguments?"

#### `delightql-error://semantic/constraint/` --- Domain Rule Violations {.dqlh}

The query is valid and all names resolve with correct arity, but a
domain-specific rule is violated.

| URI | Condition | Trigger |
|-----|-----------|---------|
| `delightql-error://semantic/constraint` | Any constraint violation | |
| `delightql-error://semantic/constraint/pivot` | Pivot requirements not met | missing IN predicate, duplicate column |
| `delightql-error://semantic/constraint/destructuring` | Destructuring rule violated | multiple `~>`, comparison in pattern |
| `delightql-error://semantic/constraint/join` | Join constraint violated | multiple full outer, missing condition |
| `delightql-error://semantic/constraint/context` | Context-aware function misuse | typo, wrong args, missing marker |
| `delightql-error://semantic/constraint/unsupported` | Construct not supported in this position | IN in projection, EXISTS in CASE |

**Why `constraint` replaces `validation`.** The word `constraint`
names what went wrong: a domain rule was violated. Pivot requires an
IN predicate. Destructuring forbids comparisons. Full outer join
cannot have multiple targets. These are specific rules, not generic
"validation."

#### `delightql-error://semantic/limitation/` --- Known Limitations {.dqlh}

| URI | Condition |
|-----|-----------|
| `delightql-error://semantic/limitation` | Any known limitation |
| `delightql-error://semantic/limitation/qualified_name_ambiguity` | Grammar ambiguity with qualified names ending in `.` |
| `delightql-error://semantic/limitation/not_implemented` | Feature not yet implemented |

### `ddl/` --- DDL Errors {.dqlh}

DDL errors are structurally similar to DQL errors but fewer in number.

| URI | Condition |
|-----|-----------|
| `ddl/parse` | DDL syntax failure |
| `ddl/semantic/resolution` | Referenced entity not found |
| `ddl/semantic/constraint` | DDL rule violated (circular dependency, duplicate definition) |

### `dml/` --- DML Errors {.dqlh}

| URI | Condition |
|-----|-----------|
| `dml/parse` | DML syntax failure |
| `dml/semantic/resolution` | Target entity not found |
| `dml/semantic/constraint` | DML rule violated |

### `database/` and `io/` --- Runtime Errors {.dqlh}

These errors occur during query execution, not compilation. They
do not belong to a language domain.

| URI | Condition |
|-----|-----------|
| `database` | Any database operation error |
| `database/connection` | Connection lock poisoned |
| `io` | I/O error |


## Implementation Notes {.dqlh}

The current implementation derives subcategories from error message
keywords for `ValidationError`, `TransformationError`, and
`TranspilationError`. Stable, static error types (`TableNotFoundError`,
`ColumnNotFoundError`) already carry precise URIs. A planned refactor
will add explicit subcategory fields to all dynamic error types,
making URIs independent of message text.
