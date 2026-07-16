# Rename Cover {.dqlh}

The RENAME-COVER operator `*(  )`{.delightql .sigil} renames specified columns while passing all others through:

```delightql
employee_2019(*)
  |> *( FamilyName as LastName)
```

```sql
select
    EmployeeId,
    FamilyName as LastName,
    FirstName,
    Title,
    ReportsTo,
    BirthDate,
    HireDate,
    Address,
    City,
    State,
    Country,
    PostalCode,
    Phone,
    Fax,
    Email
from employee_2019;
```

Rename cover preserves column count and column ordinality -- only the names change.
