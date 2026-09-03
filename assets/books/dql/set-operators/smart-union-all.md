# Smart Union All (`|;|`{.delightql .sigil}) {.dqlh}

Aligns by name, but requires both relations to have identical column count and names. Unlike
SQL's `UNION/UNION-ALL` position is irrelevant. The resulting schema
is adopted from the first relation:

```delightql
employee_2019(*)
  |;|  employee_2018(*)
  |;|  employee_2018(*)
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
UNION ALL
SELECT
  EmployeeId, LastName,
  FirstName, Title, ReportsTo,
  BirthDate, HireDate,
  Address, City, State,
  Country, PostalCode, Phone,
  Fax, Email
FROM employee_2018
UNION ALL
SELECT
  EmployeeId, LastName,
  FirstName, Title, ReportsTo,
  BirthDate, HireDate,
  Address, City, State,
  Country, PostalCode, Phone,
  Fax, Email
FROM employee_2018;
```
