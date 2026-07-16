# Group By {.dqlh}

`Group by` extends distinct with aggregation. The AGG-AND sigil `~>`{.delightql
.sigil} separates grouping columns (left) from reduced columns (right):

```delightql
employee(*)
  |> %(Department ~>  count:(*) , sum:(Salary) )
```



```sql
select
  Department,  -- grouping column
  count(*),    -- reduced column
  sum(Salary)  -- reduced column
from employee
  group by Department;
```

Grouping columns may be expressions:

```delightql
employee(*)
    |> %( Salary > 50000  as high_low,
          upper:(Department) ~>
            count:(*) ,
            avg:(Salary) )
```


```sql
select
  Salary > 50000 as high_low, -- grouping column
  upper(Department),  -- grouping column
  count(*),    -- reduced column
  avg(Salary)  -- reduced column
from employee
  group by upper(Department), (Salary > 50000) ;
```
