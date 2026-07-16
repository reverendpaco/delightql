# Embedding {.dqlh}

The EMBED operator `+(  )`{.delightql .sigil} adds a new column to a relation, placed after existing columns:

```delightql
employee_2019(*)
  |> +(  strftime:('%Y',BirthDate) - 2 as two_years_before_birth )
```

```sql
select
    *,
    strftime('%Y',BirthDate) - 2 as two_years_before_birth
from employee;
```

This is equivalent to `|> (*, expr as name)`{.delightql }. The embed syntax
makes the intent explicit: the relation is unchanged except for the added
column.
