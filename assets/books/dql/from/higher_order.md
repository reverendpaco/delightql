# Higher-Order Predicates {.dqlh}



Delightql supports higher-order predicates--predicates that accept tables (or
    scalars) as parameters and return a table. SQL calls these *table-valued
functions*.

```delightql
clean_employees(hr.employee(*))(*)
```

Here, `clean_employees` is a higher-order predicate that takes `hr.employee(*)` as its
parameter.

The transpiled SQL depends on how the predicate was defined. [Defining
higher-order rules is covered in the DDL section.]{.sidenote} Given this
definition:

```{.delightql .am}
clean_employees(T(*))( * ) :-
  T(*) as t
    |> $(trim:())( t.LastName, t.FirstName)
    |> $(to_iso:())( t.BirthDate, t.HireDate)
    |> -( SSN)
```

the query `clean_employees(hr.employee(*))(*)` produces:

```sql
select
    EmployeeId,
    trim(LastName)    as LastName,
    trim(FirstName)   as FirstName,
    Title,
    ReportsTo,
    to_iso(BirthDate) as BirthDate,
    to_iso(HireDate)  as HireDate,
    Address,
    City,
    State,
    Country,
    PostalCode,
    Phone,
    Fax,
    Email
from employee;
```

Higher-order predicates are structurally typed by the columns they reference.
[This resembles duck typing: delightql has no formal type layer, but detects
which columns the predicate body requires. Any table providing those columns is
a valid argument.]{.sidenote} In this example, any table with `LastName`,
`FirstName`, `BirthDate`, `HireDate`, and `SSN` qualifies:

```delightql
clean_employees(batch.employee_2019(*))(*)
```

The pipeline form (covered later) is equivalent:


```delightql
batch.employee_2019(*)
    |> clean_employees(*)
```
