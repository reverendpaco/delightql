# Map Covering {.dqlh}

The MAP-COVER operator `$( · )( · )`{.delightql .sigil} applies a function across specified columns while preserving all others:

```delightql
employee(*)
  |> $(upper:())( LastName, FirstName, Title, ReportsTo)
```

```sql
select
    EmployeeId,
    upper(LastName)  as  LastName,
    upper(FirstName) as  FirstName,
    upper(Title)     as  Title,
    upper(ReportsTo) as  ReportsTo,
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
from employee;
```

The first parentheses contain the function; the second lists the target
columns. The function `upper:()`{.delightql .sigil} is written as arity-0 -- a curried form where the
column value fills the implicit first argument. [This notation borrows from
Elixir's pipe conventions.]{.sidenote}

Map covering:

 1. Applies the function to each listed column
 1. Renames results to their original column names
 1. Passes through unlisted columns unchanged
 1. Preserves column ordinality

Because unlisted columns pass through, transformations can be chained:

```delightql
employee(*)
  |> $(upper:())( LastName, FirstName, Title, ReportsTo)
  |> $(to_iso:())( BirthDate, HireDate)
```

**Composing functions**. When multiple functions apply to the same columns, three options exist:

Chained covers (repetitive but clear):

```delightql
employee(*)
  |>  $(upper:())(FirstName,LastName)
  |>  $(trim:())(FirstName,LastName)
```

Containment composition using F-PARAM `@`{.delightql .sigil} as a placeholder:

```delightql
employee(*)
  |>  $(trim:(upper:(@)) )(FirstName,LastName)
```

Pipe composition using F-PIPE `/->`{.delightql .sigil}:

```delightql
employee(*)
 |>  $(upper:() /-> trim:() )(FirstName,LastName)
```

All three produce:

```sql
select
    EmployeeId,
    trim(upper(LastName))  as  LastName,
    trim(upper(FirstName)) as  FirstName,
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
from employee;
```
