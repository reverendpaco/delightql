# Argumentative Projection {.dqlh}

Columns can be projected by position using argumentative access. The FULL sigil `_`{.delightql .sigil} discards unwanted positions:

```delightql
employee(employee_id , last_name , _ , _ ,
        _ , _ , _ , _ , _ ,
        _ , _ , _ , _ , _ , _ )
```

```sql
select
  employee_id,
  last_name
from employee;
```


Only the first two columns are retained; the rest are projected away. The
identifiers `employee_id` and `last_name` name the columns in the result --
matching the underlying column names here, though positional access can also
rename by using a different identifier.

As with argumentative positional access, this notation is brittle for wide
tables -- prefer named projection when arity exceeds a handful of columns (see
[Argumentative Positional Access](Argumentative Positional Access)).
