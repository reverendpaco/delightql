# Conditional Covering (If-Only) {.dqlh}

The IF-ONLY sigil `|`{.delightql .sigil} constrains which rows a cover
applies to. Rows not matching the predicate pass through unchanged.

**Map-cover with if-only:**

```delightql
employee(*)
  |> $(upper:())(LastName, FirstName | Department = "Executive")
```

```sql
SELECT
  EmployeeId,
  CASE
    WHEN Department = 'Executive' THEN upper(
      LastName
    )
    ELSE LastName
  END AS LastName,
  CASE
    WHEN Department = 'Executive' THEN upper(
      FirstName
    )
    ELSE FirstName
  END AS FirstName,
  Title,
  Department,
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
FROM employee;
```

The predicate follows the column list, separated by `|`. This mirrors
the aggregate if-only `count:(col | pred)` -- the `|` always sits between
the operands and the condition.

Without if-only, the function applies to all rows. With if-only, the
function applies only to matching rows; non-matching rows retain their
original values.

**Basic-cover with if-only:**

```delightql
employee(*)
  |> $$("REDACTED" as Phone, "---" as Fax | Department = "Executive")
```

```sql
select
    EmployeeId,
    LastName,
    FirstName,
    Title,
    Department,
    ReportsTo,
    BirthDate,
    HireDate,
    Address,
    City,
    State,
    Country,
    PostalCode,
    case when Department = 'Executive'
         then 'REDACTED' else Phone end as Phone,
    case when Department = 'Executive'
         then '---' else Fax end        as Fax,
    Email
from employee;
```

The predicate goes at the end of the item list, after the last `as` target.

**Composability**. If-only composes with function composition and chaining:

```delightql
employee(*)
  |> $(upper:() /-> trim:())(FirstName, LastName | Country = "USA")
```

If-only is syntactic sugar over CASE expressions. The equivalent without
if-only:

```delightql
employee(*)
  |> $$( _:(Department = "Executive" -> upper:(LastName);  _ -> LastName)  as LastName,
         _:(Department = "Executive" -> upper:(FirstName); _ -> FirstName) as FirstName)
```
