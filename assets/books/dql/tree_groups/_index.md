
# Tree Groups {.dqlh}

Tree grouping transforms flat relations into nested JSON structures. Each
nesting level corresponds to a `GROUP BY` -- the tree's shape reflects the
grouping hierarchy.

![Nested Tree Grouping](images/tree-group-construction.svg)
Delightql provides this capability through compound data constructors (`{ }`,
`[ ]`) used in reduction positions. The resulting JSON is not general-purpose
-- it maps relations to a *tree normal form* where each level represents a
distinct grouping context.

Two forms exist:

- **Data-oriented**: produces arrays of objects; grouping columns become object fields
- **Metadata-oriented**: produces objects with data values as keys; a single column's values become the key names

Full JSON functionality remains available through the target SQL's native
functions (`json_object`, `json_array`, etc.).

## Compound Data Constructors (Recap) {.dqlh}

| Constructor | Scalar Position         | Aggregate Position |
|-------------|-------------------------|--------------------|
| `{ }`       | Record (string-indexed) | Table of records   |
| `[ ]`       | Tuple (numeric-indexed) | Table of tuples    |

: Compound data constructors by position (recap)

## Aggregation boundaries and computed members {.dqlh}

After `~>`, `{…}` and `[…]` are collecting aggregates. Grouping keys to the
left select their input groups; explicit keys do not turn a collector into a
scalar summary record. Ordinary scalar expressions may occur in its members:

```delightql
invoice(*) ~> { billing_country, "upper": upper:(billing_country) } as report
```

An ordinary aggregate inside a collected row refuses, with or without keys:

```{.delightql .bad}
invoice(*) ~> { billing_country, "revenue": sum:(total) }
```

```{.delightql .bad}
invoice(*) |> %(billing_country ~> { "revenue": sum:(total) })
```

Write sibling aggregates, or explicitly stage the first result:

```delightql
invoice(*) |> %(billing_country ~> { invoice_id, total } as items, sum:(total) as revenue)
invoice(*) |> %(billing_country ~> sum:(total) as revenue) ~> { billing_country, revenue } as report
```

A window expression supplies one value per input row and therefore composes
inside a collector, including through scalar expressions:

```delightql
invoice(*) ~> { invoice_id, "running": sum:(total <~ %(billing_country), #(invoice_id)) } as report
```

Its meaning agrees with computing `running` in an earlier embed stage and
then collecting `{invoice_id, running}`. Required SQL stages realize the authored
window; they do not invent grouping keys. Nested tree grouping retains its
explicit hierarchy, as described below. Normative details:
`SEMANTICS/tree-group-evaluation-law.md`.

## Tree Group Syntax {.dqlh}

Nested tree groups are created by nesting compound constructors with `~>`{.delightql .sigil}
introducing each level:
```delightql
employee(*)
  ~> { title,
       "people": ~> {first_name, last_name},
       city } as people_by_title_and_city
```

**Reading the syntax.** The `~>`{.delightql} marks tree group boundaries. Columns between a
`~>`{.delightql} and either the next `~>`{.delightql} or a closing enclyph (`}`, `]`, `)`) belong to
that level's group:

:::::{.widen}
```delightql
employee(*)
// level 1               level 2           L2 end   L1 end
// start                 start
// ↳                     ↳                      ↱        ↱
   ~> { title, "people": ~> {first_name, last_name},  city }
```
:::::::

- `title` and `city` belong to level 1 (the top-level tree group)
- `first_name` and `last_name` belong to level 2 (nested within level 1)

The grouping is hierarchical: level 2 groups are computed *within* each
distinct combination of level 1 columns.

### Terminology {.dqlh}

- **tree group**: The set of columns whose distinct combinations form one level of the tree
- **tree group variables**: The columns belonging to a tree group
- **nested tree group**: A tree group inside another tree group
- **tree group induction**: Using a compound constructor in reduction position to create an interior table
