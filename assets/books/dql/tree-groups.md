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

## Tree Group Syntax {.dqlh}

Nested tree groups are created by nesting compound constructors with `~>`{.delightql .sigil}
introducing each level:
```delightql
employee(*)
  ~> { Title,
       "people": ~> {FirstName, LastName},
       State } as people_by_title_and_state
```

**Reading the syntax.** The `~>`{.delightql} marks tree group boundaries. Columns between a
`~>`{.delightql} and either the next `~>`{.delightql} or a closing enclyph (`}`, `]`, `)`) belong to
that level's group:

:::::{.widen}
```delightql
// level 1               level 2             L2 end    L1 end
// start                 start
// ↳                     ↳                        ↱         ↱
   ~> { Title, "people": ~> {FirstName, LastName},  State }
```
:::::::

- `Title` and `State` belong to level 1 (the top-level tree group)
- `FirstName` and `LastName` belong to level 2 (nested within level 1)

The grouping is hierarchical: level 2 groups are computed *within* each
distinct combination of level 1 columns.

### Terminology {.dqlh}

- **tree group**: The set of columns whose distinct combinations form one level of the tree
- **tree group variables**: The columns belonging to a tree group
- **nested tree group**: A tree group inside another tree group
- **tree group induction**: Using a compound constructor in reduction position to create an interior table

## Data-Oriented Tree Grouping {.dqlh}

Data-oriented tree grouping uses `~>`{.delightql} followed by a compound constructor. The
result is an array of objects (or tuples), one per distinct combination of tree
group variables.

**Simple example:**
```delightql
employee(*)
  ~> { Title, State } as title_and_state
```

Returns one row containing an array of all distinct `{Title, State}`
combinations.

**Nested example:**

```delightql
employee(*)
  ~> { Title,
       "people": ~> {FirstName, LastName},
       State } as people_by_title_and_state
```

Returns a single-row, single-column table:


+----------------------------------------------------------------+
| people_by_title_and_state                                      |
+================================================================+
|  ```                                                           |
|     [                                                          |
|      { "Title": "Account Representative",                      |
|        "State": "PA",                                          |
|        "people": [                                             |
|          { "FirstName": "Stafani", "LastName": "Hurton" },     |
|          { "FirstName": "Jenda", "LastName": "Bownd" }         |
|        ]                                                       |
|      },                                                        |
|      { "Title": "Programmer",                                  |
|        "State": "PA",                                          |
|        "people": [                                             |
|          { "FirstName": "Clareta", "LastName": "Cuss" }        |
|        ]                                                       |
|      },                                                        |
|      { "Title": "Programmer",                                  |
|        "State": "GA",                                          |
|        "people": [                                             |
|          { "FirstName": "Anita", "LastName": "Aburrow" }       |
|        ]                                                       |
|      },                                                        |
|      { "Title": "VP",                                          |
|        "State": "OH",                                          |
|        "people": [                                             |
|          { "FirstName": "Drusi", "LastName": "Sachno" }        |
|        ]                                                       |
|      },                                                        |
|      { "Title": "VP",                                          |
|        "State": "PA",                                          |
|        "people": [                                             |
|          { "FirstName": "Frazer", "LastName": "Vido" },        |
|      { "FirstName": "Corney", "LastName": "Treherne" }         |
|                               ]                                |
|      }                                                         |
|    ]                                                           |
|   ```                                                          |
+----------------------------------------------------------------+
: {#tbl:array-tree-group}

**Transpilation.** Tree grouping uses JSON aggregation functions as
intermediates:
```sql
SELECT
  json_group_array(
    json_object(
      'Title', Title,
      'State', State,
      'people', people
    )
  ) AS people_by_title_and_state
FROM (
  SELECT
    Title,
    State,
    json_group_array(
      json_object('FirstName', FirstName, 'LastName', LastName)
    ) AS people
  FROM employee
  GROUP BY Title, State
);
```

The nested `GROUP BY` mirrors the nested `~>`. Each tree group level becomes a
subquery with its own grouping and JSON aggregation. The JSON functions are
implementation details -- the result is a standard column containing structured
data.

**Three-level example:**
```delightql
employee(*)
  ~> { Title,
       "people_by_state":
         ~> { State,
              "people": ~> {FirstName, LastName} } }
    as people_by_state_within_title
```

Groups first by `Title`, then within each title by `State`, then collects
people within each state.

**Sibling tree groups:**


Multiple nested groups at the same level share their parent's context but are
otherwise independent:
```delightql
employee(*)
  ~> { Title,
       "people_by_state": ~> { State, "people": ~> {FirstName, LastName} },
       "cities": ~> [City] }
    as nested_with_siblings
```

The `people_by_state` and `cities` tree groups are siblings -- both nested
within `Title`, neither containing the other.

Sibling tree groups share their parent's context but aggregate independently.
The relationship between siblings---which person was in which city -- is not
preserved. This is inherent to the structure: siblings represent independent
projections of the grouped data. [Trees with siblings satisfy TNF-G but not
TNF-R; they cannot round-trip losslessly. (See Appendix A.)]{.sidenote}


## Metadata-Oriented Tree Grouping {.dqlh}

Metadata-oriented tree grouping elevates data values to JSON keys. A column's
distinct values become the keys of a single object rather than elements of an
array.

The syntax uses `:~>` after a bare identifier:
```delightql
employee(*)
  ~> Title: ~> {FirstName, LastName} as people_by_title
```

The result is an interior record (one object), not an interior table (array of
objects):
```json
{
  "General Manager": [
    { "FirstName": "Andrew", "LastName": "Adams" }
  ],
  "IT Manager": [
    { "FirstName": "Michael", "LastName": "Mitchell" }
  ],
  "Sales Manager": [
    { "FirstName": "Nancy", "LastName": "Edwards" }
  ]
}
```

**Distinguishing syntax:**

- Normal keys are quoted strings: `"people":`
- Metadata keys are bare identifiers followed by `:~>`{.delightql}: `Title: ~>`{.delightql}


**Restriction:** Only one column can serve as a metadata key per level -- the
object can have only one set of keys. This constraint reflects JSON's
structure: two metadata-keyed objects with the same key type would create
ambiguous destructuring. Metadata-oriented trees satisfy TNF-M. (See Appendix
A.)


**Within a regular group by:**
```delightql
employee(*)
  |> %( State
          ~>
        Title: ~> {FirstName, LastName} as people_by_title )
```

Returns one row per state, each containing an object keyed by title.

## Tree Distinction {.dqlh}

Tree structures can serve as grouping columns, enabling aggregation alongside
hierarchical output:
```delightql
employee(*)
  |> %( { Title,
          "people": ~> {FirstName, LastName},
          State } as people_by_title_and_state
          ~>
        sum:(Salary), count:(*) )
```


**Restriction:** Columns referenced in nested tree groups cannot also appear as
explicit grouping columns:

```{.delightql .bad}
// INVALID: LastName appears in tree group and as grouping column
employee(*)
  |> %( { Title, "people": ~> {FirstName, LastName}, State } as tree,
        LastName
          ~>
        sum:(Salary) )
```

Columns not referenced in the tree may be added:
```delightql
employee(*)
  |> %( { Title, "people": ~> {FirstName, LastName}, State } as tree,
        DepartmentId
          ~>
        sum:(Salary), count:(*) )
```

## Tree Destructuring {.dqlh}

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

## Pathing in Tree Patterns {.dqlh}

Destructuring patterns support direct pathing, eliminating the need to match
intermediate structure. The pathing syntax (`.path.to.field`) reaches into
nested JSON without declaring every level.

**Basic pathing:**
```delightql
_(json @ {"name": "app", "config": {"server": {"port": 3000}}})
  |> (json:{.config.server.port})
```

The path `.config.server.port` extracts the value directly.

**Pathing in destructuring:**

Instead of matching the full structure:
```delightql
j ~= { name, "config": { "server": { port, host }, "database": { url } } }
```

Path directly to what you need:
```delightql
j ~= {
  name,
  .config.server.port,
  .config.server.host,
  .config.database.url
}
```

**Pathing with rename:**

Combine pathing with `as` to name the output column:
```delightql
user_data ~= ~> {
  country,
  .name_info.last_name as ln,
  .name_info.first_name as fn
}
```

**Mixed matching and pathing:**

Structural matching and pathing can combine in a single pattern:
```delightql
j ~= {
  name,
  version,
  .dependencies.react,
  .dependencies.next
}
```

Here `name` and `version` match top-level keys directly; the `.dependencies.*`
paths reach into nested structure.

**Pathing in projection:**

Pathing works outside destructuring patterns, in normal projection:
```delightql
_(json @ {"name": "app", "scripts": {"dev": "next dev", "build": "next build"}})
  |> ({
    "name": json:{.name},
    "scripts": json:{.scripts}
  })
```


## Interior Drill-Down {.dqlh}

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

## Narrowing {.dqlh}

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
