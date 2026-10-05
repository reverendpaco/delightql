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
// should fail: table does not exist
nonexistent_table(*) (~~error://semantic/resolution/table ~~)

// should fail: column not in scope
customer(*) |> (no_such_column) (~~error://semantic/resolution/column ~~)

// should fail: any semantic error (prefix match)
invoice(*), total in (1,2,3) (~~error://semantic ~~)
```

A bare error annotation with no URI matches any error:

```delightql
// should fail with some error, don't care which
bad_query(*) (~~error ~~)
```

Errors are rarely needed for end users.

## URI Prefix Matching {.dqlh}

The URI is matched as a prefix against the actual error's canonical
URI. `error://semantic/resolution` matches `semantic/resolution/table`,
`semantic/resolution/column`, and any future `semantic/resolution/*` error.
`error://semantic/arity` matches only `semantic/arity` and its
sub-paths.

## Error URI Categories {.dqlh}

Each `DelightQLError` variant maps to a canonical URI path. The URI
is a stable identifier for the error category, reusable in
documentation, tooling, and diagnostics.

| URI | Meaning |
|-----|---------|
| `authored` | A termination the program itself demanded. |
| `client` | An incident of the interactive client itself. |
| `configuration` | A host's boot settings are invalid. |
| `dml` | A data-modification query violated DML shape rules. |
| `imprint` | A blueprint or manifest lifecycle refusal. |
| `internal` | A defect in DelightQL itself. |
| `namespace` | A namespace-creation policy refusal. |
| `operational` | This session refuses to run a valid query. |
| `parse` | The source text is structurally invalid. |
| `runtime` | An execution-time failure. |
| `semantic` | The query is well-formed but semantically invalid. |
| `target` | The foreign engine rejected or failed the query. |

: Top-level error URI families (`dql explain delightql-error://<family>` lists each family's members)


## Coexistence with Data Assertions {.dqlh}

Error assertions and data assertions can appear in the same file,
documenting both correct and incorrect forms:

```delightql
// correct: an ordinary assertion effect checks the established relation
invoice(*), total > 0
  !> assert!(exists(*), "a positive-total row exists")(*)

// incorrect: commas produce a multi-column single-row relation
invoice(*), total in (1,2,3) (~~error://semantic/membership ~~)
```

## Scope {.dqlh}

Error assertions are primarily a language development tool.
They assert contracts about the compiler's behavior.
End users writing queries against a
database should have no use for expected failures. [I think]{.sidenote}
