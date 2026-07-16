# Distinct {.dqlh}

The GROUP-MODULO operator `%(  )`{.delightql .sigil}
returns distinct combinations of the specified columns:

```delightql
employee(*)
  |> %(Department)
```


```sql
select
  distinct Department
from employee;
```

Multiple columns return distinct combinations:

```delightql
employee(*)
  |> %(Department, State)
  |> #(Department,State descending)
```

```sql
select
  distinct Department, State
from employee
    order by Department asc, State desc;
```

To deduplicate all columns -- converting a multiset (bag) into a set:

```delightql
employee(*)
  |> %(*)  //returns unique rows and removes duplicates
```
