# Set Operators {.dqlh}

SQL's set operators **UNION**, **INTERSECT** and **EXCEPT** all operate on
union-compatible schemas with ordinality-based matching.
This means that to use these operators in SQL you need
to arrange an enumeration of columns from each table
via position and type only, **and** that the number of columns
must be the same.


![Union Compatability via Ordinal Alignment](images/union-compatabile.svg)



It is a somewhat interesting corner of SQL as nowhere
else (with the exception of the occasional `group by 2`{.sql})
does SQL use the ordinality of a relation's columns.

Delightql, of course, supports all of these operators
but opens up the door to other functionality by

  1. relaxing the same number requirement and permitting ragged unions and intersects
  2. having a preference for naming-based access


To make this more concrete, consider delightql's **UNION CORRESPONDING**:

```delightql
  users(*) ; users_2024(*)
```

The semicolon sigil `;`{.delightql .sigil} of **UNION CORRESPONDING** separates
two tables much like a comma would for joining.  With this
operator columns are matched by their names and a super set of both
tables' columns are synthesized.

![Union Compatability via Name Alignment With OUTER](images/union-corresponding.svg)


Delightql re-introduces[ The SQL standard actually defines a `CORRESPONDING BY`
clause (SQL-92 (ISO/IEC 9075:1992)) that can be used with `INTERSECT`, `UNION`,
and `EXCEPT`, though, as usual, few actually implemented it. It's even a
reserved word.]{.sidenote} this CORRESPONDING *name-based alignment mode* as an
alternative to SQL's normal UNION-like *ordinal alignment*.  Delightql prefers
name-based alignment but still offers ordinal alignment for backwards compatibility.


Delightql's UNION-like operators are the following:


| Mode | Sigil | Alignment | Schema requirement |
|------|-------|-----------|-------------------|
| Corresponding | `;` | By name | Any (NULL-padded) |
| Smart | `|;|` | By name | Identical names and same column count |
| Positional | `||` | By ordinal | Same column count |

: Set operator alignment modes

