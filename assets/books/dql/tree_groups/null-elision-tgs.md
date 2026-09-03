
## Null Elision in Tree Groups {.dqlh}

When an outer join feeds into a tree group, the join pads unmatched rows with
NULLs. In a flat relation this is the only way to represent "no match" -- NULL
serves as a sentinel for absence. Trees have no such limitation: an empty array
`[]` directly represents "no children."

Tree groups decode this flat-world encoding. When **all** value columns in a
contributing row are NULL, the row is elided from the array. The result is `[]`,
not `[{"col": null, ...}]`.

**Example:**

```delightql
parents(*) : _(id, name ---- 1, "Alice"; 2, "Bob"; 3, "Charlie")
children(*) : _(parent_id, toy ---- 1, "doll"; 1, "ball"; 2, "car")

parents(*), children?(*), parents.id = children.parent_id
  |> %(name ~> {toy} as toys)
```

Charlie has no children. The outer join produces `(3, "Charlie", NULL)`. Without
null elision, the tree group would produce:

| name    | toys              |
|---------|-------------------|
| Alice   | [{"toy":"doll"},{"toy":"ball"}] |
| Bob     | [{"toy":"car"}]   |
| Charlie | [{"toy":null}]    |

With null elision:

| name    | toys              |
|---------|-------------------|
| Alice   | [{"toy":"doll"},{"toy":"ball"}] |
| Bob     | [{"toy":"car"}]   |
| Charlie | []                |

**Scope.** Null elision applies to both forms:

- `~> {a, b, c}` -- curly (object) tree groups
- `~> [a, b, c]` -- bracket (tuple) tree groups

**The elision rule.** A row is elided when *every* value column in the
constructor is NULL. If any value column is non-null, the row is preserved. This
means a row like `{"name": "Alice", "age": null}` survives -- only fully-null
rows (the signature of outer-join padding) are dropped.

**Parent entity preservation.** Null elision never removes the parent entity.
Charlie still appears in the result -- only the *contents* of the nested array
change (from `[{"toy":null}]` to `[]`). The **GROUP BY** produces a row for
Charlie's grouping key; the aggregate column becomes an empty array rather than
an array containing a null-valued object.

**Round-trip behavior.** Destructuring an empty array produces zero rows.
After null elision, destructuring Charlie's `[]` eliminates Charlie from the
flat result -- which is the same behavior as an inner join.
