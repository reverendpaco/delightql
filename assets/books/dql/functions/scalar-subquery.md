# Scalar Subquery {.dqlh}

Scalar subqueries return a single value (one row, one column) usable anywhere a
column is valid. Delightql transforms a relation into a scalar subquery by
postfixing its name with `:`{.delightql .sigil} and using interior notation.

**Uncorrelated**. The subquery is independent of the outer query:

```{.delightql .numberLines}
employee(*)
    |> (FirstName,
        LastName,
        Salary,
        employee:( ~> avg:(Salary)) as AvgSalary)
```

```sql
select
  FirstName,
  LastName,
  Salary,
  (select avg(Salary) from employee) as AvgSalary
from employee;
```

The F-COLON sigil `:`{.delightql .sigil} after the relation name signals a scalar subquery. The
interior notation--here `~> avg:(Salary)`{.delightql} -- must produce exactly one row and one
column.

**Correlated**. The subquery references values from the outer query. Use an explicit condition to correlate on a column:

:::::{.widen}
`tpt:#numbering_on()`
```{.delightql .numberLines}
employee(*) as e
    |> (FirstName,
        LastName,
        Salary,
        employee:( ~> avg:(Salary)) as AvgSalary,
        employee:( , DepartmentName = e.DepartmentName ~> avg:(Salary)) as AvgSalaryInDept)
```
`tpt:#numbering_off()`
:::::::


```sql
select
  FirstName,
  LastName,
  Salary,
  (select avg(Salary) from employee) as AvgSalary,
  (select avg(Salary) from employee
      where DepartmentName = e.DepartmentName) as AvgSalaryInDept
from employee e;
```
