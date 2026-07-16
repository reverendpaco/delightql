# Basic Dimension Covering {.dqlh}

The BASIC-COVER operator `$$(  )`{.delightql .sigil} transforms individual columns without the curried function syntax:

```delightql
employee(*)
  |> $$( "--------" as Phone, upper:(State) as State)
```

```sql
select
    EmployeeId,
    LastName,
    FirstName,
    Title,
    ReportsTo,
    BirthDate,
    HireDate,
    Address,
    City,
    upper(State) as State,
    Country,
    PostalCode,
    '--------' as Phone,
    Fax,
    Email
from employee;
```

Each transformed column requires an `as` modifier -- this identifies which columns
are being replaced. Unlisted columns pass through in their original ordinality.
Referencing a nonexistent column is an error.
