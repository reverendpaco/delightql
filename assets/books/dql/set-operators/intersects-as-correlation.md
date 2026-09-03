
# Intersects via correlation {.dqlh}

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

