
# Interior Drill-Down {.dqlh}

Tree destructuring with `~=`{.delightql} requires the user to spell out the interior
schema -- every level of nesting must be declared in the pattern. When the
schema is statically known (tree groups from a view, CTE, or inline query),
`.column(*)` provides a shorter, self-documenting alternative.

**Syntax.** `.column_name(*)`{.delightql} as a suffix on any relation expression. The
`(*)`{.delightql} means "all columns of the interior relation." Argumentative
access is also supported: `.entities(name, type)`{.delightql} binds the interior
relation's columns positionally, with the arity of the interior — the same
Prolog-style relation access as `employees(id, name)`{.delightql}. It is
relation access, never a projection list: to keep a subset of a wider
interior, expand with `(*)`{.delightql} and project ordinarily, or use brace
narrowing. The operator is chainable: `.entities(*).columns(*)`{.delightql}.

**Context carry-forward.** Outer columns remain available after a drill-down.
`.entities(*)`{.delightql} produces entity-level columns *plus* all columns from the
enclosing level, minus the exploded column itself. This is lateral-join
semantics -- each interior row inherits the context of its parent row.

**Cardinality.** Expansion is correlated: each parent row contributes one
output row per row of ITS interior, so the total is the sum over parent
rows of `cardinality(r.t)`{.delightql}. Duplicate interior rows are preserved. A
NULL or empty interior IS empty — it contributes ZERO rows, in every
expansion form; interior expansion is not an outer join, and a parent
with no children vanishes rather than surviving as a row of NULL
children. (This is the expansion half of the round-trip law: construction
elides all-NULL contributor rows to `[]`{.delightql}, and `[]`{.delightql} expands to
nothing.)

**Post-pipe narrowing.** The same parenthesized access in post-pipe
position performs the SAME correlated expansion and retains only the
interior heading:

```delightql
R(*).t(*)          // expand t; keep R's context beside each child
R(*) |> .t(*)      // the same expansion; keep only t's columns
R(*) |> .t(a, b)   // argumentative narrowing (positional bind)
```

The two forms agree on row count, duplicate multiplicity, parent/child
correspondence, and empty/NULL interiors; they differ only in retained
context — `R(*) |> .t(*)`{.delightql} is exactly `R(*).t(*) |> (t.*)`{.delightql}.
Parenthesized access — postfix or post-pipe, glob or argumentative —
requires a statically known tree-group interior; over external JSON every
parenthesized form is refused, because the compiler cannot discover keys
at runtime and plan a heading from them. External JSON narrows with
braces, whose members are the programmer's static heading witness
(declared fields become the planned columns; extra runtime keys never do;
a missing declared key yields NULL without suppressing the row).

**Example -- CTE drill-down:**

```delightql
users(*) |> %(country ~> {first_name, last_name} as people) : by_country
by_country(*).people(*)
```

This produces one row per person, with `country` carried forward from the
grouping level.

**Example -- chained drill-down:**

```delightql
main::(*).entities(*).columns(*)
  , entity_name = "users"
```

Each `.name(*)`{.delightql} step explodes one level of nesting. Columns from all prior
levels remain available for filtering.

**Equivalence with `~=`{.delightql}.** The same query written both ways:

```delightql
// Drill-down form:
main::(*) |> (entities) .entities(*)

// Equivalent ~= form:
main::(*)
  , entities ~= ~> {name, type, doc, "columns": ~> {col_name, col_type, col_pos}}
  |> -(entities)
```

The drill-down form does not require the user to know the interior schema.

