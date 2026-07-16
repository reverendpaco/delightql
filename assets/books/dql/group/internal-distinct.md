# Internal Distinct {.dqlh}

Some aggregates accept a distinct modifier on their input. The INNER-MODULO sigil `%`{.delightql .sigil} prefixes the column:

```{.delightql .numberLines}
employee(*)
    |>  %( Department ~>
            count:(%LastName) ,
            count:(%BirthDate))
```


```sql
select
  Department,
  count(distinct LastName),
  count(distinct BirthDate)
from employee
  group by Department;
```
