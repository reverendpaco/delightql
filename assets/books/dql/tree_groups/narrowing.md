
# Narrowing {.dqlh}

The `~=`{.delightql} operator and interior drill-down both carry context forward --
outer columns survive into the result. This is the correct default for
relational composition, but it requires projecting out the intermediate
columns when they are no longer needed:

```delightql
j(*), j ~= {.packages} |> -(j)
  , packages ~= ~> {.version, .name, .description} |> -(packages)
```

When the intent is to drill into a column, extract fields, and discard
everything else, the `.column{...}`{.delightql} operator expresses this more efficiently:

```delightql
j(*)
  |> .j{.packages}
  |> .packages{.version, .name, .description}
```

Each step replaces the current row with the destructured result.

**When to use which.**

| Form           | Carries context | Use case                                                  |
|----------------|-----------------|-----------------------------------------------------------|
| `~= pattern`   | Yes             | General relational destructuring; join with outer columns |
| `.col(*)`      | Yes             | Drill-down when schema is known; outer columns needed     |
| `|> .col(*)`   | No              | Same expansion as drill-down, interior heading only (schema-known) |
| `.col{...}`    | No              | Navigate into nested JSON; only interior fields matter — and the REQUIRED form for external JSON (static heading witness) |

**Example -- cargo metadata:**

```delightql
j(*)
  |> (j:{.packages} as packages)
  |> .packages{.version, .name, .description}
```

The path extraction `j:{.packages}`{.delightql} pulls the packages array out of the
top-level object; then `.packages{...}`{.delightql} iterates and extracts fields.
The result is a flat table with `version`, `name`, and `description`
columns -- no intermediate columns to clean up.

