# Project Out {.dqlh}

The PROJECT-OUT operator [ -(◌) ]{.sidesigil} subtracts columns from a relation:

```delightql
employee(*)
  |> -(birth_date, email)
```

```sql
select
    employee_id,
    last_name,
    first_name,
    title,
    reports_to,
    --  birth_date, -- column projected out
    hire_date,
    address,
    city,
    state,
    country,
    postal_code,
    phone,
    fax
    -- Email -- column projected out
from employee;
```

All columns except `birth_date` and `email` are retained. This is particularly
useful for wide tables where listing retained columns would be tedious.
