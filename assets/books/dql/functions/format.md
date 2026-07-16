# Format Function and Strings {.dqlh}

Format strings interpolate column values into text. The F-STRING sigil `:`{.delightql .sigil} prefixes a string literal:

`tpt:#numbering_on()`

```{.delightql .numberLines }
employee(*)
  |> +( :"{LastName}, {FirstName} making ${Salary}" as readable)
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
    Salary,
    Address,
    City,
    State,
    Country,
    PostalCode,
    Phone,
    Fax,
    Email,
    LatName || ', ' || FirstName || ' making $' || Salary as readable
from employee;
```

Braces `{ }` enclose column names.  Format strings also permit the
usage of the following escape sequences:

| escape sequence | meaning            |
|-----------------|--------------------|
| `\n`            | newline            |
| `\t`            | tab                |
| `\\`            | backslash          |
| `\q`            | single quote (`'`) |
| `\Q`            | double quote (`"`) |


**Strings in delightql are only double-quoted**.  Any double-quoted
string without a colon is a raw string and accepts neither
interpolation nor escape sequences.  Delightql also
uses the triple-double-quotes to make for easier embedding
of double quotes.  These too may be used as string
interpolators with a preceding colon.


```delightql
_(1) |> ("""
  This is a banner and
  has no \n as a newline escape
  but the ones you typed are
  there because they are the bytes
  you typed
  """)
```

```delightql
_(1) |> (:"""
  This is a banner and
  HAS the \n as a newline escape
  AND the ones you typed are
  there because they are the bytes
  you typed.  Also, you can type a
  double-quote as " or \Q and a
  single-quote as ' or \q
  """)
```
