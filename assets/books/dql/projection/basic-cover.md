# Basic Dimension Covering {.dqlh}

The BASIC-COVER operator `$$(  )`{.delightql .sigil} transforms individual columns without the curried function syntax:

```delightql
employee(*)
  |> $$( "--------" as phone, upper:(state) as state)
```

```sql
select
    employee_id,
    last_name,
    first_name,
    title,
    reports_to,
    birth_date,
    hire_date,
    address,
    city,
    upper(state) as state,
    country,
    postal_code,
    '--------' as Phone,
    fax,
    email
from employee;
```

Each transformed column requires an `as` modifier -- this identifies which columns
are being replaced. Unlisted columns pass through in their original ordinality.
Referencing a nonexistent column is an error.
