# Error Assertions {.dqlh}

Error assertions verify that a query fails compilation or resolution
with a specific error. They use a separate annotation with the reserved
name `error`:

```delightql
(~~error://uri/path ~~)
```

The URI identifies the expected error category using a hierarchical
path. The pipeline attempts to compile the query; if compilation
fails and the actual error matches the URI, the assertion passes.

```delightql
-- should fail: table does not exist
nonexistent_table(*) (~~error://resolution/table_not_found ~~)

-- should fail: column not in scope
users(*) |> (no_such_column) (~~error://resolution/column_not_found ~~)

-- should fail: any validation error (prefix match)
users(*), age in (1,2,3) (~~error://validation ~~)
```

A bare error annotation with no URI matches any error:

```delightql
-- should fail with some error, don't care which
bad_query(*) (~~error ~~)
```

Errors are rarely needed for end users.

## URI Prefix Matching {.dqlh}

The URI is matched as a prefix against the actual error's canonical
URI. `error://resolution` matches `resolution/table_not_found`,
`resolution/column_not_found`, and any future `resolution/*` error.
`error://validation/arity` matches only `validation/arity` and its
sub-paths.

## Error URI Categories {.dqlh}

Each `DelightQLError` variant maps to a canonical URI path. The URI
is a stable identifier for the error category, reusable in
documentation, tooling, and diagnostics.

| URI | Phase | Meaning |
|-----|-------|---------|
| `parse` | compile | Syntax-level parse failure |
| `resolution/table_not_found` | compile | Table not in schema |
| `resolution/column_not_found` | compile | Column not in scope |
| `validation/arity` | compile | Wrong number of arguments |
| `validation/ambiguous` | compile | Ambiguous column reference |
| `validation/duplicate` | compile | Duplicate name or definition |
| `build/*` | compile | AST construction errors |
| `transform/*` | compile | SQL generation errors |
| `limitation/*` | compile | Known limitations |
| `runtime/bug` | runtime | Generated SQL rejected by backend (compiler defect) |
| `runtime/collision` | runtime | Namespace or resource already exists (duplicate mount!/consult!) |
| `runtime/useafterfree` | runtime | Accessing parted or unavailable resource (use after part!) |
| `runtime/assertion` | runtime | Data assertion verdict is fail |

: Error URI categories


## Coexistence with Data Assertions {.dqlh}

Error assertions and data assertions can appear in the same file,
documenting both correct and incorrect forms:

```delightql
-- correct: an ordinary assertion effect checks the established relation
users(*), age > 0
  !> assert!(exists(*), "a positive-age row exists")(*)

-- incorrect: commas produce a multi-column single-row relation
users(*), age in (1,2,3) (~~error://validation ~~)
```

## Scope {.dqlh}

Error assertions are primarily a language development tool.
They assert contracts about the compiler's behavior.
End users writing queries against a
database should have no use for expected failures. [I think]{.sidenote}
