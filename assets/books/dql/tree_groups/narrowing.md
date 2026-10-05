
# Narrowing {.dqlh}

The `~=`{.delightql} operator and interior drill-down both carry context forward --
outer columns survive into the result. This is the correct default for
relational composition, but it requires projecting out the intermediate
columns when they are no longer needed:

```delightql
partner_sale(*), payload ~= {.items} |> -(payload)
  , items ~= ~> {.track_id, .price, .quantity} |> -(items)
```

When the intent is to drill into a column, extract fields, and discard
everything else, the `.column{...}`{.delightql} operator expresses this more efficiently:

```delightql
partner_sale(*)
  |> .payload{.items}
  |> .items{.track_id, .price, .quantity}
```

Each step replaces the current row with the destructured result.

**When to use which.**

| Form           | Carries context | Use case                                                  |
|----------------|-----------------|-----------------------------------------------------------|
| `~= pattern`   | Yes             | General relational destructuring; join with outer columns |
| `.col(*)`      | Yes             | Drill-down when schema is known; outer columns needed     |
| `|> .col(*)`   | No              | Same expansion as drill-down, interior heading only (schema-known) |
| `.col{...}`    | No              | Navigate into nested JSON; only interior fields matter — and the REQUIRED form for external JSON (static heading witness) |

**Example -- partner sale items:**

```delightql
partner_sale(*)
  |> (payload:{.items} as items)
  |> .items{.track_id, .price, .quantity}
```

The path extraction `payload:{.items}`{.delightql} pulls the items array out of the
top-level object; then `.items{...}`{.delightql} iterates and extracts fields.
The result is a flat table with `track_id`, `price`, and `quantity`
columns -- no intermediate columns to clean up.

