# Rename {.dqlh}

Rename a column during projection with `as`:

```delightql
employee(*)
  |>  (first_name as f, last_name)
```

```sql
select
  first_name as f,
  last_name
from employee;
```
