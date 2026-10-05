
# Tree Destructuring {.dqlh}

Tree destructuring is the inverse of tree grouping -- it flattens nested JSON
back into rows.

![group and destructure](images/tree-group-destructure.svg)

The TREE-UNIFY sigil `~=`{.delightql .sigil} matches a JSON column against a
destructuring pattern:

```delightql
partner_sale(*)
  , payload ~= { partner_order,
                 "buyer": { email, country },
                 "items": ~> { track_id, price, quantity } }
  |> -(payload)
```

The pattern syntax mirrors construction syntax. Each `~>` level multiplies rows
-- the result is the Cartesian product of all nested arrays.

**Array vs object matching:**
```delightql
// Matches an ARRAY of objects  --  multiplies rows by array length
partner_sale(*) |> (sale_id, payload:{.items} as items)
  , items ~= ~> { track_id, quantity }

// Matches a single OBJECT  --  extracts fields, no multiplication
partner_sale(*) |> (sale_id, payload:{.buyer} as buyer)
  , buyer ~= { email, country }
```

The `~>` in destructuring means "iterate over this array," just as in
construction it means "aggregate into this array."

**Renaming during destructuring:**

The string key matches the JSON; the identifier after `:` names the output
column:
```delightql
partner_sale(*)
  , payload ~= { partner_order,
                 "buyer": { email, country },
                 "items": lines }
  |> -(payload)
```

Here `"items"` matches the JSON key; `lines` becomes the column name. The
`lines` column contains the nested array as-is, not destructured.

**Staged destructuring:**

Destructure incrementally by chaining `~=` operations:
```delightql
partner_sale(*)
  , payload ~= {partner_order, "items": lines}
  , lines ~= ~> {track_id, price, quantity}
  |> -(payload)
```

The first `~=` extracts `partner_order` and keeps `lines` as a JSON array. The
second destructures `lines` into individual rows. Stop at any level to
preserve nested structure.

**Metadata-oriented destructuring:**

The `:~>` syntax works symmetrically -- object keys become column values:
```delightql
partner_sale(*) |> (sale_id, payload:{.payout} as payout)
  , payout ~= ~> country: ~> _
  |> -(payout)
```

Given `payout`, an object keyed by country codes, this extracts the key into a
`country` column; `_` disregards the amount under each key.

**Binding semantics:**

Column names in the pattern match JSON keys by name. If the pattern says
`email` and the JSON has `"email"`, they bind. A mismatched name
produces nulls -- there is no compile-time validation against JSON structure.

