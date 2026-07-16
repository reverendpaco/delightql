# Project Retain {.dqlh}

```delightql
employee(*)
  |>  (FirstName , LastName)
```

```Sql
select
  FirstName,
  LastName
from employee;
```

The R-PIPE `|>`{.delightql .sigil} passes a relation to the PROJECT operator `( )`{.delightql .sigil}. Columns listed
inside are retained; all others are discarded.

The pipe creates a scope barrier: columns to the left are no longer in scope
after the projection. Only the projected columns continue forward.

Projections can be chained:

```delightql
employee(*)
  |>  (FirstName , LastName)
  |>  (FirstName )
```

``` sql
select FirstName from employee;
-- -- optimized from:
-- select FirstName
--   from (
--     select
--       FirstName,
--       LastName
--     from employee);
```

Delightql (and SQL optimizers) will simplify redundant intermediate
projections. But scope is enforced at each step--this will not work:

```{.delightql .numberLines .bad}
// Error: FirstName not in scope
employee(*)
  |>  (LastName)
  |>  (FirstName)
```

After line 2, only LastName exists in the piped relation.
