# Filter Clauses {.dqlh}

The IF-ONLY sigil `|`{.delightql .sigil} constrains which values enter an aggregate:

```delightql
employee(*)
  |>  %( Department ~>
         count:(%LastName) ,
         count:(%BirthDate),
         count:(LastName | length:(LastName) > 10)
            as long_lastname_count)
```

For dialects supporting `FILTER`:

```sql
select
  Department,
  count(distinct LastName),
  count(distinct BirthDate),
  count(LastName)
    filter
      (where length(LastName) > 10) as long_lastname_count
from employee
  group by Department;
```

For dialects without `FILTER`, delightql emits a `CASE` expression:

```sql
select
  Department,
  count(distinct LastName),
  count(distinct BirthDate),
  count(case when length(LastName) > 10
            then LastName else null) as  long_lastname_count
from employee
  group by Department;
```
