# Project Out {.dqlh}

The PROJECT-OUT operator [ -(◌) ]{.sidesigil} subtracts columns from a relation:

```delightql
employee(*)
  |> -(BirthDate, Email)
```

```sql
select
    EmployeeId,
    LastName,
    FirstName,
    Title,
    ReportsTo,
    --  BirthDate, -- column projected out
    HireDate,
    Address,
    City,
    State,
    Country,
    PostalCode,
    Phone,
    Fax,
    -- Email
from employee;
```

All columns except `BirthDate` and `Email` are retained. This is particularly
useful for wide tables where listing retained columns would be tedious.
