# Window/Analytic Functions {.dqlh}

Window functions combine aggregation with per-row results. They aggregate over
a dynamic window but return one value per row -- still scalar functions by
definition. [Array languages are the closest analog in other programming
paradigms.]{.sidenote}

```delightql
employee(*)
    |> (EmployeeId,
        DepartmentId,
        Salary,
        dense_rank:( <~ %(DepartmentId),#(Salary)) as ranking )
```

```sql
SELECT
  EmployeeId,
  DepartmentId,
  Salary,
  dense_rank() OVER (
    PARTITION BY
      DepartmentId
    ORDER BY Salary
  ) AS ranking
FROM employee;
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
| *n* | *n* preceding |
| `+`*n* | *n* following |
| `-`*n* | *n* preceding (explicit) |

: Window frame bound syntax

`Examples:`

```delightql
  ntile:( 10  <~  %(DepartmentId),#(-Salary), groups(_,_))
  ntile:( 10  <~  %(DepartmentId),#(-Salary), groups(+1,_))
  ntile:( 10  <~  %(DepartmentId),#(-Salary), rows(1,.))
  ntile:( 10  <~  %(DepartmentId),#(-Salary), rows(_,-(upto*2)))
  ntile:( 10  <~  %(DepartmentId),#(-Salary), range(.,upto*2) )
```


:::::{.widen}
```sql
  ntile(10) over
    ( partition by DepartmentId order by Salary desc
      groups between
        unbounded preceding and unbounded following)
  ntile(10) over
    ( partition by DepartmentId order by Salary desc
      groups between 1 following and unbounded following)
  ntile(10) over
    ( partition by DepartmentId order by Salary desc
      rows between 1 preceding and current row)
  ntile(10) over
    ( partition by DepartmentId order by Salary desc
      rows between unbounded preceding and (upto*2) preceding)
  ntile(10) over
    ( partition by DepartmentId order by Salary desc
      range between current row and (upto*2) following)
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
