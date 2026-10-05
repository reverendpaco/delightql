# Regular Expression Column Addressing {.dqlh}

A regular expression can select columns by name pattern:

```delightql
employee(*)
  |> ( /date/ )
```

```sql
select
    birth_date,   -- matches /date/
    hire_date     -- matches /date/
from employee;
```

The pattern matches anywhere in a column's name, and it ignores case, as a
column reference does: `(Birth_Date)` and `/BIRTH/` both find
`birth_date`. The regex applies only to column names, not namespaces or
indexes.

To match case exactly, add the flag `c` after the closing slash:

```delightql
employee(*)
  |> ( /date/c )
```

Every column in the example database is lowercase, so `/date/c` still
finds `birth_date` and `hire_date`, while `/Date/c` finds nothing and
refuses.

`c` is the only flag. `/date/i` refuses, because ignoring case is already
the default. Ignoring case applies inside character classes too, so
`/^[A-Z]/` matches `birth_date`, while `/^[A-Z]/c` matches nothing here.

The pattern language is a small subset of POSIX basic regular expressions:
`^` and `$` anchor, `.` matches any character, `*` repeats, `[...]` is a
character class, and `\+` and `\?` repeat one-or-more and zero-or-one.
There are no groups and no alternation.

**Positions.** A regex stands wherever a list of columns is enumerated:
**PROJECT-IN** `( )`, **PROJECT-OUT** `-( )`, the second parentheses of
**MAP-COVER**, the source of a **RENAME-COVER** `*(/date/ as :"d_{@}")`,
**GROUP-MODULO** keys, function arguments, and record members. A
project-in that matches no column refuses; a project-out may match none.

It cannot:

- Be followed by `as`; rename the matched columns with a name template
- Stand as a single value: an operand, a function-pipe step, or a slot
- Add columns in **EMBED** `+(  )`, since it names only columns that already exist
