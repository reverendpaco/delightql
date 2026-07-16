# Rename {.dqlh}

Rename a column during projection with `as`:

```delightql
employee(*)
  |>  (FirstName as f, LastName)
```

```Sql
select
  FirstName as f,
  LastName
from employee;
```
