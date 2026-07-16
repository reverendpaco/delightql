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



## Union All Corresponding (`;`{.delightql .sigil}) {.dqlh}

Aligns by name, NULL-padding missing columns. Output schema: first relation's
columns, then non-overlapping columns from the second.[This is closer in definition to OUTER
UNION. The few SQLs that implement UNION ALL CORRESPONDING do so by
outputting the intersection of the two column sets, instead of the union of the
two column sets that delightql favors.]{.sidenote}

```delightql
 _( a,b,c
    -------
    1,2,3;
    4,5,6)
    ;
 _( d,   a,b
   -------
   "foo",10,20;
   "bar",40,50)
```

a | b | c | d
--|---|---|--
1 | 2 | 3 | NULL
4 | 5 | 6 | NULL
10 | 20 | NULL | foo
40 | 50 | NULL | bar

Union All Corresponding is a *ragged* union.

## Smart Union All (`|;|`{.delightql .sigil}) {.dqlh}

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


## Positional Union All (`||`{.delightql .sigil}) {.dqlh}

**Aligns by position**. Requires identical column count. Useful for intentional realignment.[The below
example uses interior relations to shape each relation prior to the `UNION
ALL`.]{.sidenote}:

```delightql
users_2024(|> (last_name,first_name,age))
  ||
users_2023(|> (LastName,First,Age))
```


## Set Semantics vs Multiset Semantics {.dqlh}

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


## Intersects via correlation {.dqlh}

Having introduced the operators above, one would
assume that a new sigil for intersects is in the offing.
Instead delightql chooses to reuse the same
syntax for correlations to represent a statement of
which columns to intersect on.

![Intersect **ON** via correlation conditions](images/interesct-corresponding.svg)

After any union-flavored multiset operator, conjoin a condition that correlates
the previous relations together.  From such a union an intersection results:

```delightql
employee_2019(*) as e1 |;|
  employee_2018(*) as e2,
  e1.EmployeeId = e2.EmployeeId
```


SQL's `INTERSECT` only matches on the entire tuple, it lacks an `INTERSECT
ON/BY` parameterization. Delightql's correlation syntax lets you choose which
columns to match on -- a per-column intersection that SQL cannot express
without rewriting the query as a pair of `EXISTS` subqueries.


### Correlation syntax matches alignment mode {.dqlh}

The correlation condition should use the same addressing schema as the alignment:

- Name-based modes (`;`{.delightql .sigil}, `|;|`{.delightql .sigil}) use
  name-based correlation: `x.col = y.col` or `x.* = y.*`
- Positional mode (`||`{.delightql .sigil}) uses positional correlation:
  `x|1| = y|1|` or `x|*| = y|*|`

The full-tuple shorthand `x.* = y.*` means "match on all column names that
appear in both x and y." Columns present on only one side are ignored for
matching. Under `|;|`{.delightql .sigil} this distinction is moot since the schemas
are identical. Under `;`{.delightql .sigil}
the schemas may be different, and matching on the intersection of names is
the only natural reading.

The positional shorthand `x|*| = y|*|` means "match on all column positions."



+-------------------------------+-------------------------------------+
| DQL                           | Equivalent SQL concept              |
+===============================+=====================================+
|                               |                                     |
| ```                           |                                     |
| x(*) ; y(*) ,x.* = y.*        |   INTERSECT ALL CORRESPONDING       |
| ```                           |                                     |
+-------------------------------+-------------------------------------+
|                               |                                     |
| ```                           |                                     |
| x(*) |;| y(*) ,x.* = y.*      |   INTERSECT ALL                     |
| ```                           |                                     |
|                               |  (Name safe)                        |
+-------------------------------+-------------------------------------+
|                               |                                     |
| ```                           |                                     |
| x(*) || y(*) ,x|*| = y|*|     |   INTERSECT ALL                     |
| ```                           |                                     |
|                               |  (positional,                       |
|                               |    = SQL's `INTERSECT ALL`)         |
+-------------------------------+-------------------------------------+
|                               |                                     |
| ```                           |                                     |
| x(*) || y(*) ,x|*| = y|*|     |   INTERSECT ALL                     |
| ```                           |                                     |
|                               |  (positional,                       |
|                               |    = SQL's `INTERSECT ALL`)         |
+-------------------------------+-------------------------------------+
|                               |                                     |
| ```delightql                  | Per-column intersection             |
| x(*) |;| y(*) ,x.id = y.id    |   (no SQL equivalent)               |
| ```                           |                                     |
+-------------------------------+-------------------------------------+


: Intersection as union with correlation


To belabor a point, intersection re-purposes correlation syntax that is
seen most often  with *joins* to be useful for *intersection*.
To see the difference between a join and an intersection look at how
the two tables prior are combined:

+-------------------------------+--------------------------------+
| Correlation as JOIN ON        | Correlation as INTERSECT ON    |
+===============================+================================+
|                               |                                |
| ```                           |   ```                          |
|                               |                                |
| employee_2019(*) as e1,       |   employee_2019(*) as e1 |;|   |
|   employee_2018(*) as e2,     |     employee_2018(*) as e2,    |
|   e1.id=e2.id                 |     e1.id=e2.id                |
|                               |                                |
| ```                           |   ```                          |
+-------------------------------+--------------------------------+
| `,` between the two tables    | `|;|` between the two tables   |
| produces a **join** (rows     | produces an **intersection**   |
| are paired).                  | (rows are filtered).           |
+-------------------------------+--------------------------------+

: Correlation as join versus correlation as intersect



> **Equality and NULL-safety.** The `=` in both columns above looks identical,
> but the compilation differs. In join position (left column), `=`
> compiles to SQL `=` -- NULLs do not match, because NULL-to-NULL
> matching in a join can explode row counts . In set
> correlation position (right column), `=` compiles to
> `IS NOT DISTINCT FROM` -- NULLs match. This is safe because set
> correlation filters via `EXISTS`, which tests for the presence of a
> matching row without multiplying output.


> **How intersection is executed in SQL**.
>
>
> Example:
>
> ```dql
> users_2023(*) as u23 ; users_2024(*) as u24,
>   u23.email = u24.email
> ```
>
> ```sql
> SELECT
>   id, first_name, last_name, email, age,
>   status, country, balance, NULL, NULL, NULL
> FROM users_2023 AS u23
>   WHERE EXISTS (SELECT 1
>     FROM (
>       SELECT
>         id, first_name, last_name, email, NULL,
>         status, NULL, NULL, department,
>         salary, created_at
>       FROM users_2024 AS u24
>     ) AS t1
>     WHERE outer_0.email IS NOT DISTINCT FROM t1.email)
>
> UNION ALL
>
> SELECT
>   id, first_name, last_name, email, NULL,
>   status, NULL, NULL, department, salary,
>   created_at
> FROM users_2024 AS u24
>   WHERE EXISTS (SELECT 1
>     FROM (
>       SELECT
>         id, first_name, last_name, email, age,
>         status, country, balance, NULL, NULL, NULL
>       FROM users_2023 AS u23
>     ) AS t0
>     WHERE t0.email IS NOT DISTINCT FROM outer_1.email)
> ```


## Minus (Except) {.dqlh}

Minus returns rows from the first relation that have no match in the second.
A single operator `-`{.delightql .sigil} aligns by name:

```delightql
employee_current(*) - employee_terminated(*)
```

Rows in `employee_current` with no corresponding row (by name) in `employee_terminated`.
Schemas must align -- if column names differ, rename first:

```delightql
employee_current(*) - employee_terminated(|> *(emp_id as id))
```
