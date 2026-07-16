# Regular Expression Column Addressing {.dqlh}

A regular expression can select columns by name pattern:

```delightql
employee(*)
  |> ( /Date/ )
```

```sql
select
    BirthDate,   -- matches Regex /Date/
    HireDate     -- matches Regex /Date/
from employee;
```

Both `BirthDate` and `HireDate` match the pattern `/Date/`. The regex applies only to
column names (not namespaces or indexes). Delightql uses UNIX BRE syntax within
the REGEX sigil `/  /`{.delightql .sigil}. Append `i` for case-insensitive matching: `/date/i`.

**Restrictions**. Regex column addressing may only appear in **PROJECT-IN**, **PROJECT-OUT**, or the second parentheses of **MAP-COVER**. It cannot:

- Be followed by `as`
- Be passed directly to a function
- Appear in **EMBED** `+(  )`, **BASIC-COVER** `$$(  ),` **RENAME-COVER** `*(  )`, or **GROUP-MODULO**
