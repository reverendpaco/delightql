
# Tree Destructuring {.dqlh}

Tree destructuring is the inverse of tree grouping -- it flattens nested JSON
back into rows.

![group and destructure](images/tree-group-destructure.svg)

The TREE-UNIFY sigil `~=`{.delightql .sigil} matches a JSON column against a
destructuring pattern:

```delightql
table_with_json(*)
  , people_by_state_within_title ~= ~> { Title,
             "people_by_state":
               ~> { State,
                    "people": ~> {FirstName, LastName} } }
  |> -(people_by_state_within_title)
```

The pattern syntax mirrors construction syntax. Each `~>` level multiplies rows
-- the result is the Cartesian product of all nested arrays.

**Array vs object matching:**
```delightql
// Matches an ARRAY of objects  --  multiplies rows by array length
p ~= ~> { Title, State }

// Matches a single OBJECT  --  extracts fields, no multiplication
p ~= { Title, State }
```

The `~>` in destructuring means "iterate over this array," just as in
construction it means "aggregate into this array."

**Renaming during destructuring:**

The string key matches the JSON; the identifier after `:` names the output
column:
```delightql
, people_by_state_within_title ~= ~> { Title,
       "people_by_state": ~> { State, "people": peeps } }
```

Here `"people"` matches the JSON key; `peeps` becomes the column name. The
`peeps` column contains the nested array as-is, not destructured.

**Staged destructuring:**

Destructure incrementally by chaining `~=` operations:
```delightql
table_with_json(*)
  , nested ~= ~> {country, "users": sub_users}
  , sub_users ~= ~> {FirstName, LastName}
  |> -(nested)
```

The first `~=` extracts `country` and keeps `sub_users` as a JSON array. The
second destructures `sub_users` into individual rows. Stop at any level to
preserve nested structure.

**Metadata-oriented destructuring:**

The `:~>` syntax works symmetrically -- object keys become column values:
```delightql
temp(*), json_col ~= ~> country: ~> {FirstName, LastName}
  |> -(json_col)
```

Given an object keyed by country names, this extracts the key into a `country`
column and iterates the nested arrays.

**Binding semantics:**

Column names in the pattern match JSON keys by name. If the pattern says
`FirstName` and the JSON has `"FirstName"`, they bind. A mismatched name
produces nulls -- there is no compile-time validation against JSON structure.

