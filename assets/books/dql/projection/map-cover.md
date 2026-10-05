# Map Covering {.dqlh}

The MAP-COVER operator `$( · )( · )`{.delightql .sigil} applies a function across specified columns while preserving all others:

```delightql
employee(*)
  |> $(upper:())( last_name, first_name, title, city)
```

```sql
select
    employee_id,
    upper(last_name)  as  last_name,
    upper(first_name) as  first_name,
    upper(title)     as  title,
    reports_to,
    birth_date,
    hire_date,
    address,
    upper(city)      as  city,
    state,
    country,
    postal_code,
    phone,
    fax,
    email
from employee;
```

The first parentheses contain the function; the second lists the target
columns. The function `upper:()`{.delightql .sigil} is written with its
argument row open -- the column value lands in the row's final argument, the
same landing a function pipe takes. For a unary function the final argument
is the only one; for `max:(0)` the column value lands after the written
`0`, as `max(0, value)`. Write `@` where the value must land elsewhere.

Map covering:

 1. Applies the function to each listed column
 1. Renames results to their original column names
 1. Passes through unlisted columns unchanged
 1. Preserves column ordinality

Because unlisted columns pass through, transformations can be chained:

```delightql
employee(*)
  |> $(upper:())( last_name, first_name, title, city)
  |> $(date:())( birth_date, hire_date)
```

**Composing functions**. When multiple functions apply to the same columns, three options exist:

Chained covers (repetitive but clear):

```delightql
employee(*)
  |>  $(upper:())(first_name,last_name)
  |>  $(trim:())(first_name,last_name)
```

Containment composition using F-PARAM `@`{.delightql .sigil} as a placeholder:

```delightql
employee(*)
  |>  $(trim:(upper:(@)) )(first_name,last_name)
```

Both produce:

```sql
select
    employee_id,
    trim(upper(last_name))  as  last_name,
    trim(upper(first_name)) as  first_name,
    title,
    reports_to,
    birth_date,
    hire_date,
    address,
    city,
    state,
    country,
    postal_code,
    phone,
    fax,
    email
from employee;
```
