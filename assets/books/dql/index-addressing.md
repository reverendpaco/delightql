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
    employee_id, -- at position 1
    last_name,   -- at position 2
    first_name   -- at position 3
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
  phone,  -- position -3
  fax,    -- position -2
  email   -- position -1
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
customer(*)
  |> ( |1:3| )
```

```sql
SELECT customer_id, first_name, last_name
FROM customer;
```

Either bound may be omitted. An open start means "from the first column"; an
open end means "through the last column":

```delightql
customer(*)
  |> ( |:3| )           // first three columns
```

```sql
SELECT customer_id, first_name, last_name
FROM customer;
```

```delightql
customer(*)
  |> ( |5:| )           // fifth column onward
```

```sql
SELECT address, city, state, country, postal_code, phone, fax, email, support_rep_id
FROM customer;
```

Negative indices count from the end, following the same convention as single
ordinals:

```delightql
customer(*)
  |> ( |-3:-1| )        // last three columns
```

```sql
SELECT fax, email, support_rep_id
FROM customer;
```

```delightql
customer(*)
  |> ( |:-2| )          // all but the last column
```

```sql
SELECT customer_id, first_name, last_name, company, address, city, state, country,
       postal_code, phone, fax, email
FROM customer;
```

Ranges can be scoped to a table alias, just like single ordinals:

```delightql
customer(*) as c
  |> ( c|1:3|, c|6:8| )
```

```sql
SELECT customer_id, first_name, last_name, city, state, country
FROM customer AS c;
```

Ranges compose with other operators. For example, EMBED-MAP can apply a
function across a range of columns:

```delightql
track(*)
  |> +$(:( @ + 100) as :"{@}_offset")(|7:9|)
```

| Syntax | Meaning |
|--------|---------|
| `|1:3|` | Columns 1 through 3 |
| `|5:|` | Column 5 through last |
| `|:3|` | First through column 3 |
| `|-3:-1|` | Third-to-last through last |
| `|:-2|` | First through second-to-last |
| `c|1:3|` | Columns 1–3 of alias `c` |

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
customer(*) as c,
  employee(*) as e, e.employee_id=c.support_rep_id
  |> (  |12| as email,
        c|1| as customer_id,
        e|-1| as rep_email)
```

Unscoped indices refer to the total column order across all joined tables. The
following addressing schemes are available:


| Scheme | Example | Meaning |
|--------|---------|---------|
| Total | `|14|` | 14th column overall |
| Total reverse | `|-5|` | 5th from end overall |
| Scoped | `e|1|` | 1st column of `e` |
| Scoped reverse | `e|-1|` | Last column of `e` |
| Named | `e.email` | By name |

: Index addressing schemes

**When index notation breaks down**. Total indexing across joins depends on column
counts and join order. For this reason, it's probably wise to
reserve index notation for exploration, and managing hostile schemas.


## Reposition Operator {.dqlh}

The REPOSITION operator `*[column as position]`{.delightql .sigil} moves columns
to specific positions without removing any columns.

```delightql
invoice_line(*) |> *[unit_price as 1]
```
```sql
SELECT unit_price, invoice_line_id, invoice_id, track_id, quantity FROM invoice_line;
-- Before: (invoice_line_id, invoice_id, track_id, unit_price, Quantity)
-- After:  (unit_price, invoice_line_id, invoice_id, track_id, Quantity)
```

The column `unit_price` moves to position 1; all other columns shift to accommodate.

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
invoice_line(*) |> *[invoice_line_id as -1]
```

Moves `invoice_line_id` to the last position:
```text
Before: (invoice_line_id, invoice_id, track_id, unit_price, quantity)
After:  (invoice_id, track_id, unit_price, quantity, invoice_line_id)
```

### Multiple Repositions {.dqlh}

Multiple columns can be repositioned in a single operation:
```delightql
invoice_line(*) |> *[unit_price as 1, quantity as 2]
```
```text
Before: (invoice_line_id, invoice_id, track_id, unit_price, quantity)
After:  (unit_price, quantity, invoice_line_id, invoice_id, track_id)
```

Columns are placed in the order specified; remaining columns fill the gaps.
