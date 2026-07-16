# Addressing Columns by Index {.dqlh}

Named columns are preferred for clarity, but some schemas resist naming--wide
CSVs, auto-generated headers, legacy tables with hundreds of columns. For
these, delightql provides *index notation*.

```delightql
employee(*)
  |> ( |1|, |2|, |3| )
```



```sql
select
    EmployeeId, -- at position 1
    LastName,   -- at position 2
    FirstName   -- at position 3
from employee;
```

The INDEX enclyph `|  |`{.delightql .sigil} encloses an integer literal (no
expressions). Indices are 1-based; negative indices count from the end.

```delightql
employee(*)
  |> (|-3|, |-2|, |-1|)
```

```sql
SELECT
  Phone,  -- position -3
  Fax,    -- position -2
  Email   -- position -1
FROM employee;
```


> Indexing conventions. Column indices are 1-based, following SQL's ORDER BY 1
> convention. Array pathing is 0-based, following JSON/JavaScript convention.
> The first column is `|1|`{.delightql .sigil}; the first array element is
> `[0]`{.delightql .sigil}.

## Column Ranges {.dqlh}

A COLUMN RANGE `|start:end|`{.delightql .sigil} selects a contiguous slice of
columns by position. Both bounds are inclusive and 1-based.

```delightql
users(*)
  |> ( |1:3| )
```

```sql
SELECT id, first_name, last_name
FROM users;
```

Either bound may be omitted. An open start means "from the first column"; an
open end means "through the last column":

```delightql
users(*)
  |> ( |:3| )           -- first three columns
```

```sql
SELECT id, first_name, last_name
FROM users;
```

```delightql
users(*)
  |> ( |5:| )           -- fifth column onward
```

```sql
SELECT age, status, country, created_at, last_login, balance
FROM users;
```

Negative indices count from the end, following the same convention as single
ordinals:

```delightql
users(*)
  |> ( |-3:-1| )        -- last three columns
```

```sql
SELECT created_at, last_login, balance
FROM users;
```

```delightql
users(*)
  |> ( |:-2| )          -- all but the last column
```

```sql
SELECT id, first_name, last_name, email, age, status, country, created_at, last_login
FROM users;
```

Ranges can be scoped to a table alias, just like single ordinals:

```delightql
users(*) as u
  |> ( u|1:3|, u|5:7| )
```

```sql
SELECT id, first_name, last_name, age, status, country
FROM users AS u;
```

Ranges compose with other operators. For example, EMBED-MAP can apply a
function across a range of columns:

```delightql
users(*)
  |> +$(:( @ + 100) as :"{@}_offset")(|2:5|)
```

| Syntax | Meaning |
|--------|---------|
| `|1:3|` | Columns 1 through 3 |
| `|5:|` | Column 5 through last |
| `|:3|` | First through column 3 |
| `|-3:-1|` | Third-to-last through last |
| `|:-2|` | First through second-to-last |
| `u|1:3|` | Columns 1–3 of alias `u` |

: Column range syntax summary

**When ranges break down.** The same caveats as single ordinals apply: schema
changes silently shift what a range covers. Prefer named columns for stable
queries; reserve ranges for exploration and hostile schemas.

Index notation works with **PROJECT-OUT**, **RENAME-COVER**, **MAP-COVER**,
**GROUP-MODULO**, and other operators:

```delightql
employee(*)
  |> -( |1|, |2| , |-2| )
```


**Scoped Index Notation**

In joins, indices can be scoped to a table alias:

```delightql
employee(*) as e,
  department(*) as d, d.DepartmentName=e.DepartmentName
  |> (  |14| as email,
        e|1| as EmployeeId,
        d|-4| as DepartmentName)
```

Unscoped indices refer to the total column order across all joined tables. The
following addressing schemes are available:


| Scheme | Example | Meaning |
|--------|---------|---------|
| Total | `|14|` | 14th column overall |
| Total reverse | `|-5|` | 5th from end overall |
| Scoped | `e|1|` | 1st column of `e` |
| Scoped reverse | `e|-1|` | Last column of `e` |
| Named | `e.Email` | By name |

: Index addressing schemes

**When index notation breaks down**. Total indexing across joins depends on column
counts and join order. For this reason, it's probably wise to
reserve index notation for exploration, and managing hostile schemas.


## Reposition Operator {.dqlh}

The REPOSITION operator `*[column as position]`{.delightql .sigil} moves columns
to specific positions without removing any columns.

```delightql
users(*) |> *[email as 1]
```
```sql
SELECT email, id, first_name, last_name, age FROM users;
-- Before: (id, first_name, last_name, email, age)
-- After:  (email, id, first_name, last_name, age)
```

The column `email` moves to position 1; all other columns shift to accommodate.

### Positive and Negative Positions {.dqlh}

Positions are 1-indexed. Negative positions count from the end:

| Position | Meaning |
|----------|---------|
| `1` | First |
| `2` | Second |
| `-1` | Last |
| `-2` | Second-to-last |

: Position meanings for the reposition operator

```delightql
users(*) |> *[id as -1]
```

Moves `id` to the last position:
```
Before: (id, first_name, last_name, email, age)
After:  (first_name, last_name, email, age, id)
```

### Multiple Repositions {.dqlh}

Multiple columns can be repositioned in a single operation:
```delightql
users(*) |> *[email as 1, age as 2]
```
```
Before: (id, first_name, last_name, email, age)
After:  (email, age, id, first_name, last_name)
```

Columns are placed in the order specified; remaining columns fill the gaps.
