# Boolean Expressions as Columns {.dqlh}


Predicates -- what delightql calls *sigma clauses* -- can appear in column position,
returning boolean values. [Some SQL dialects lack a boolean type; these
transpile to 1 and 0.]{.sidenote}


```delightql
employee(*)
    |> +( DepartmentCity="San Francisco"
            and Title!="Engineer"
                AS san_fran_engineer,
          DepartmentCity="San Francisco"
                AS san_fran,
          Salary > 150000
            or BonusPercentage > 200
                AS well_compensated,
          Title!="Engineer"
                AS is_engineer)
```


:::::{.widen}
```sql
  select
    *,
    DepartmentCity = 'San Francisco' and Title != 'Engineer' as san_fran_engineer,
    DepartmentCity = 'San Francisco' as san_fran,
    Salary > 150000 or BonusPercentage > 200 as well_compensated,
    Title != 'Engineer' as is_engineer
  from employee;
```
::::::


Compound predicates must use keywords (`and`, `or`) rather than sigils (
`,`{.delightql .sigil} , `;`{.delightql .sigil}) when appearing as column
expressions.


**Existence tests**. Semi-joins and anti-joins also return booleans when used in column position:

```delightql
employee(*)
 |> +( +department(,
         department.DepartmentId
          =employee.DepartmentId),
      \+ department(,
         department.DepartmentId
          =employee.DepartmentId),
      +between(Salary,50000,75000))
```


:::::{.widen}
```sql
  select
    *,
    --
    exists (select 1 from department
      where department.DepartmentId=employee.DepartmentId),
    --
    not exists (select 1 from department
      where department.DepartmentId=employee.DepartmentId),
    --
    Salary between 50000 and 75000
  from employee;
```
:::::::
