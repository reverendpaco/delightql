# Window/Analytic Functions {.dqlh}

Window functions combine aggregation with per-row results. They aggregate over
a dynamic window but return one value per row -- still scalar functions by
definition. [Array languages are the closest analog in other programming
paradigms.]{.sidenote}

```delightql
invoice(*)
    |> (invoice_id,
        customer_id,
        total,
        dense_rank:( <~ %(customer_id),#(total)) as ranking )
```

```sql
SELECT
  invoice_id,
  customer_id,
  total,
  dense_rank() OVER (
    PARTITION BY
      customer_id
    ORDER BY total
  ) AS ranking
FROM invoice;
```

The **F-OVER** sigil `<~`{.delightql .sigil} introduces the window specification. Everything before `<~`
is passed to the function; everything after defines the window frame.

**Window specification syntax**. Comma-separated, all optional:

  - `%(  )` -- partition clause (one allowed)
  - `#(  )` -- order clause (one allowed)
  - `rows(from, to)`, `range(from, to)`, or `groups(from, to)` -- frame specification (one allowed)

**Frame Bounds:**

| Syntax | Meaning |
|--------|---------|
| `.` | current row |
| `_` | unbounded |
| `+`*n* | *n* following |
| `-`*n* | *n* preceding |

: Window frame bound syntax

`Examples:`

```delightql
invoice(*)
    |> (invoice_id, customer_id, total,
        ntile:( 10  <~  %(customer_id),#(total desc), groups(_,_))      as g_all,
        ntile:( 10  <~  %(customer_id),#(total desc), groups(+1,_))     as g_after,
        ntile:( 10  <~  %(customer_id),#(total desc), rows(-1,.))       as r_prev,
        ntile:( 10  <~  %(customer_id),#(total desc), rows(_,-(2*2)))   as r_upto,
        ntile:( 10  <~  %(customer_id),#(total desc), range(.,+(2*2)) ) as rg)
```


:::::{.widen}
```sql
  ntile(10) over
    ( partition by customer_id order by total desc
      groups between
        unbounded preceding and unbounded following)
  ntile(10) over
    ( partition by customer_id order by total desc
      groups between 1 following and unbounded following)
  ntile(10) over
    ( partition by customer_id order by total desc
      rows between 1 preceding and current row)
  ntile(10) over
    ( partition by customer_id order by total desc
      rows between unbounded preceding and (2*2) preceding)
  ntile(10) over
    ( partition by customer_id order by total desc
      range between current row and (2*2) following)
```
::::::


**Default window**. For an empty window specification, use `<~`{.delightql .sigil} with nothing following:

```delightql
  employee(*)
    |> +(  row_number:( <~ ) as row_number )
```


```sql
select
  *,
  row_number() over () as row_number
from employee;
```
