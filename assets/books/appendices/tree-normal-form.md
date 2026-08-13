# Appendix: Tree Normal Form {.appendix .dqlh}

Tree grouping bridges two worlds: the relational (flat, tabular, join-oriented) and the hierarchical (nested, tree-structured, document-oriented). Not all JSON has a sensible relational interpretation -- arbitrary nesting can be too irregular to map cleanly. But some trees do, and understanding which ones helps clarify what tree grouping actually computes.

This appendix defines *tree normal forms* -- a vocabulary for describing JSON
structure and its relational interpretation. Unlike database normal forms
(which form a linear hierarchy), tree normal forms are organized as a graph.
Nodes are forms; edges are constraints or interpretations. Some edges restrict
structure; others assign meaning to nesting.

### The Graph {.dqlh}

![Mind Map](images/tnf_graph.svg)

Edges represent:

| Edge | Type | Meaning |
|------|------|---------|
| TNF-0 → TNF-T | Restriction | Structural hygiene |
| TNF-T → TNF-N | Interpretation | Nesting means namespacing |
| TNF-T → TNF-G | Interpretation | Nesting means grouping |
| TNF-T → TNF-M | Interpretation | Nesting means pivot |
| TNF-G → TNF-SR | Restriction | No nested groups (flat) |
| TNF-G → TNF-R | Restriction | Single path, no siblings |
| TNF-G → TNF-GN | Combination | Grouping with namespaced leaves |

: Tree normal form edge types

The graph admits extension. New forms slot in by defining their edges.

---

## The Forms {.dqlh}

### TNF-0: Valid JSON {.dqlh}

The baseline: any valid JSON per RFC 7159.

- Keys may be duplicated
- Arrays may be heterogeneous
- Top-level value may be a scalar
- Objects may be empty
```json
42
```
```json
[ 1, [], "a string", true, {},
  { "a": [2, 3], "a": [2, 3] },
  [ 2, 3, [ 4, 5 ] ]
]
```

**Relational interpretation:** None guaranteed. This is raw material. But it's
still queryable -- pathing and `json_each` work on any valid JSON.

---

### TNF-T: Well-Typed JSON {.dqlh}

*Restriction from TNF-0: no duplicate keys, homogeneous arrays, non-empty objects, array or object at top level.*
```json
{
  "name": "Alice",
  "scores": [95, 87, 91]
}
```

**Relational interpretation:** Arrays can be interpreted as collections; objects as records. Pathing is unambiguous. But nesting semantics are not yet defined -- is a nested object a namespace? A grouped row? A pivot?

TNF-T is the foundation for the interpretive forms that follow.

---

### TNF-N: Namespaced {.dqlh}

*Interpretation from TNF-T: nesting means semantic organization.*

Nested objects group related fields -- structure, not data rows.
```json
{
  "LastName": "eklund",
  "address": {
    "City": "boston",
    "State": "MA"
  }
}
```

The `address` object is a namespace. The tree is semantically equivalent to:
```json
{
  "LastName": "eklund",
  "address_City": "boston",
  "address_State": "MA"
}
```

**Relational interpretation:** Namespaced trees flatten to a single row. Pathing (`.address.City`) navigates the namespace.

**Trade-off:** More expressive (preserves semantic grouping) but less directly relational (requires flattening).

---

### TNF-G: Grouped {.dqlh}

*Interpretation from TNF-T: nesting means aggregation.*

Arrays represent grouped rows -- the result of `GROUP BY`.
```json
[
  { "Title": "Engineer",
    "people": [
      { "FirstName": "Alice", "LastName": "Smith" },
      { "FirstName": "Bob", "LastName": "Jones" }
    ]
  },
  { "Title": "Manager",
    "people": [
      { "FirstName": "Carol", "LastName": "White" }
    ]
  }
]
```

Each nesting level is a grouping context. The outer array groups by `Title`; the inner `people` array collects rows within each title.

**Relational interpretation:** Direct correspondence to `GROUP BY`. Construction compresses cardinality; destructuring expands it.

---

### TNF-M: Metadata-Keyed {.dqlh}

*Interpretation from TNF-T: nesting means pivot.*

Data values become object keys.
```json
{
  "Engineer": [
    { "FirstName": "Alice", "LastName": "Smith" }
  ],
  "Manager": [
    { "FirstName": "Carol", "LastName": "White" }
  ]
}
```

The keys (`Engineer`, `Manager`) are data values lifted to metadata.

**Relational interpretation:** Keys map to a column; values map to grouped rows. Destructuring recovers the key as a column value.

**Trade-off:** Convenient for lookup but constrained -- only one column can serve as keys per level. Two metadata-keyed objects with the same key type create ambiguous destructuring.

---

### TNF-SR: Simply Relational {.dqlh}

*Restriction from TNF-G: no nested groups.*

A flat array of homogeneous objects -- the simplest grouped form.
```json
[
  { "Title": "Engineer", "FirstName": "Alice", "LastName": "Smith" },
  { "Title": "Engineer", "FirstName": "Bob", "LastName": "Jones" },
  { "Title": "Manager", "FirstName": "Carol", "LastName": "White" }
]
```

No nested arrays. Each object is a row; the array is a table.

**Relational interpretation:** Direct. The JSON *is* a table in array-of-objects form. No grouping, no hierarchy -- just rows.

---

### TNF-R: Round-Trippable {.dqlh}

*Restriction from TNF-G: single path from root to deepest leaf, no sibling groups.*
```json
[
  { "Title": "Engineer",
    "State": "CA",
    "people": [
      { "FirstName": "Alice", "LastName": "Smith" }
    ]
  }
]
```

**Relational interpretation:** Lossless. `relation → tree → relation` recovers the original data (modulo column order).

**Why siblings break round-tripping:**
```delightql
employee(*) ~> { Title,
                 "people": ~> {FirstName, LastName},
                 "cities": ~> [City] }
```

Siblings aggregate independently. The join -- which person was in which city -- is not preserved. Destructuring recovers each path independently:

- `Title`, `FirstName`, `LastName` (via `people`)
- `Title`, `City` (via `cities`)

But not the original four-column row. This is TNF-G but not TNF-R.

---

### TNF-GN: Grouped with Namespaced Leaves {.dqlh}

*Combination of TNF-G and TNF-N: grouping structure with namespaced leaf objects.*
```json
[
  { "Title": "Engineer",
    "people": [
      { "name": { "first": "Alice", "last": "Smith" },
        "contact": { "email": "alice@x.com", "phone": "555-1234" }
      }
    ]
  }
]
```

The outer structure is grouped (array of objects with nested arrays). The leaf objects use namespacing (`name`, `contact`).

**Relational interpretation:** Destructure the grouping levels; flatten the namespaced leaves. The result has columns `Title`, `name_first`, `name_last`, `contact_email`, `contact_phone`.

---

## Mixing Forms {.dqlh}

Real trees often combine forms at different levels. The graph shows which combinations make sense:
```json
{
  "metadata": {
    "generated": "2024-01-15",
    "version": "1.0"
  },
  "data": [
    { "Title": "Engineer",
      "people": [
        { "FirstName": "Alice", "LastName": "Smith" }
      ]
    }
  ]
}
```

- `metadata` is TNF-N (namespacing)
- `data` is TNF-G (grouping)

The relational interpretation:

- Flatten `metadata.generated`, `metadata.version` to columns
- Destructure `data` → `data.people` to rows
- Result: one row per person, metadata fields repeated

Understanding which form applies where clarifies what operations make sense.

---

## Edge Types {.dqlh}

The graph has two kinds of edges:

**Restriction edges** add structural constraints:
- TNF-0 → TNF-T: hygiene (no dup keys, homogeneous arrays)
- TNF-G → TNF-SR: flatness (no nested groups)
- TNF-G → TNF-R: single-path (no siblings)

**Interpretation edges** assign meaning to structure:
- TNF-T → TNF-N: nesting is namespacing
- TNF-T → TNF-G: nesting is grouping
- TNF-T → TNF-M: nesting is pivot

Restriction edges constrain what trees are valid. Interpretation edges determine how to read them relationally.

---

## Summary {.dqlh}

| Form | Key Property | Relational Interpretation |
|------|--------------|---------------------------|
| TNF-0 | Valid JSON | None guaranteed; queryable via pathing |
| TNF-T | Well-typed | Arrays are collections; objects are records |
| TNF-N | Namespaced | Flatten to single row |
| TNF-G | Grouped | GROUP BY; destructure to rows |
| TNF-M | Metadata-keyed | Pivot; keys become column values |
| TNF-SR | Simply relational | Direct table (array of flat objects) |
| TNF-R | Round-trippable | Lossless construction/destruction |
| TNF-GN | Grouped + namespaced | Destructure groups, flatten namespaces |

: Summary of tree normal forms

The forms answer different questions:

- **TNF-0 / TNF-T:** Is this JSON structurally sound?
- **TNF-N / TNF-G / TNF-M:** What does nesting mean here?
- **TNF-SR / TNF-R:** How constrained is the grouping?
- **TNF-GN:** Can I mix interpretations?

Tree normal forms are not prescriptive -- TNF-0 is sometimes exactly what you need. They are a vocabulary for understanding what your tree structure means relationally, and what operations it supports.

---

## Extending the Graph {.dqlh}

The graph admits new forms by defining edges. Examples:

- **TNF-MR** (metadata-keyed, round-trippable): TNF-M + single-path constraint
- **TNF-SN** (simply namespaced): TNF-N + flat (no nested namespaces)
- **TNF-GM** (grouped + metadata): grouping with metadata-keyed intermediate levels

Each new form names a useful combination.

### Guiding Principles {.dqlh}

1. **Trees and tables mix.** Trees have a valid relational interpretation under certain structural constraints.

2. **Start with relations.** The cleanest trees arise from grouping relations, not from arbitrary JSON. Construction informs understanding.

3. **Arrays are rows; objects are records.** Arrays represent homogeneous collections (multiple rows of the same shape). Objects represent heterogeneous structure (named fields, like columns).

4. **Grouping compresses; destructuring expands.** Construction decreases cardinality (many rows → fewer rows with nested arrays). Destructuring increases cardinality (nested arrays → many rows).

5. **Siblings lose information.** Sibling tree groups aggregate independently. The relationship between siblings -- which person was in which city -- is not preserved. This is inherent, not a bug.

6. **Metadata-oriented trees are pivots.** When data values become object keys, the structure resembles a pivot table. This helps with the object-relational impedance mismatch but introduces constraints.

7. **Zeroth normal form has its place.** Arbitrary JSON can still be queried via `json_each` and pathing. Tree normal forms define what's *cleanly* relational, not what's queryable at all.
