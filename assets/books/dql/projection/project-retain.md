# Project Retain {.dqlh}

```delightql
employee(*)
  |>  (first_name , last_name)
```

```sql
select
  first_name,
  last_name
from employee;
```

The R-PIPE `|>`{.delightql .sigil} passes a relation to the PROJECT operator `( )`{.delightql .sigil}. Columns listed
inside are retained; all others are discarded.

The pipe creates a scope barrier for the relation being projected: only its
projected columns continue forward. Inside an interior relation, bindings
from the enclosing scope remain accessible; they are not part of the local
interface this projection replaces.

Projections can be chained:

```delightql
employee(*)
  |>  (first_name , last_name)
  |>  (first_name )
```

``` sql
select first_name from employee;
-- -- optimized from:
-- select first_name
--   from (
--     select
--       first_name,
--       last_name
--     from Employee);
```

Delightql (and SQL optimizers) will simplify redundant intermediate
projections. But scope is enforced at each step--this will not work:

```{.delightql .numberLines .bad}
// Error: first_name not in scope
employee(*)
  |>  (last_name)
  |>  (first_name)
```

After line 2, only last_name exists in the piped relation.
