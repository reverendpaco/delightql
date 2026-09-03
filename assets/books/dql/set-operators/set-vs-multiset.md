
# Set Semantics vs Multiset Semantics {.dqlh}

All of delightql set operators are actually
multiset operators inasmuch as they preserve duplicates.
In other words, all set operators are `ALL`-flavored.

If set semantics are required, use `DISTINCT ALL` via `|> %(*)`{.delightql}.

```delightql
employee_2019(*) |;| employee_2018(*) |> %(*)
```

```sql
SELECT
  EmployeeId, LastName,
  FirstName, Title, ReportsTo,
  BirthDate, HireDate,
  Address, City, State,
  Country, PostalCode, Phone,
  Fax, Email
FROM employee_2019
  UNION  --- NOT UNION ALL
SELECT
  EmployeeId, LastName,
  FirstName, Title, ReportsTo,
  BirthDate, HireDate,
  Address, City, State,
  Country, PostalCode, Phone,
  Fax, Email
FROM employee_2018;
```

which is equivalent to

```sql
SELECT DISTINCT * FROM
  (SELECT
    EmployeeId, LastName,
    FirstName, Title, ReportsTo,
    BirthDate, HireDate,
    Address, City, State,
    Country, PostalCode, Phone,
    Fax, Email
  FROM employee_2019
    UNION ALL
  SELECT
    EmployeeId, LastName,
    FirstName, Title, ReportsTo,
    BirthDate, HireDate,
    Address, City, State,
    Country, PostalCode, Phone,
    Fax, Email
  FROM employee_2018)
;
```

