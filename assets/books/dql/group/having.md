# Having {.dqlh}

Filter on reduced columns by placing a predicate after the `group by`:

```delightql
employee(*)
  |> %( Department ~> count:(*) as employee_count)
      ,  employee_count > 50
```

Read this as: "group employees by Department, count each group, then keep only
groups with more than 50 rows."


```sql
select
  Department,
  count(*) as employee_count
from employee
  group by Department
    having count(*) > 50;
```



> **Why does SQL have both WHERE and HAVING?**
>
> SQL has an implicit order of operations. `WHERE` filters rows before grouping;
> `HAVING` filters groups after aggregation. The two keywords signal this
> distinction. [For a historical reflection on this issue, see
> `tpt:#fc(<HAVINGBlunderfulTime>)`.]{.sidenote}
>
>
> The abstraction is leaky -- most programmers soon recognize that `HAVING` is
> equivalent to wrapping in a subquery and filtering with `WHERE`:
>
> ```sql
> SELECT Department, count(*) AS employee_count
> FROM employee
> GROUP BY Department
> HAVING count(*) > 50;
>
>
> -- equivalent to:
>
> SELECT * FROM (
> SELECT Department, count(*) AS employee_count
> FROM employee
> GROUP BY Department
> ) WHERE employee_count > 50;
> ```
>
> Because delightql has explicit order of operations, no separate syntax is
> needed. The predicate simply follows the group by:
>
> ```dql
> employee(*)
> |> %(Department ~> count:(*) as employee_count),
> employee_count > 50
> ```
>
> Placing the filter earlier would be an error, `employee_count` does not exist until after the aggregation.
