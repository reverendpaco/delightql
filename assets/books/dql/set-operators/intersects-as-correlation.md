
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
genre_2024(*) as g1 |;|
  genre_2025(*) as g2,
  g1.genre_id = g2.genre_id
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
| ```delightql                  |                                     |
| employee_2024(*) as x ;       |   INTERSECT ALL CORRESPONDING       |
|   employee_2025(*) as y,      |                                     |
|   x.* = y.*                   |                                     |
| ```                           |                                     |
+-------------------------------+-------------------------------------+
|                               |                                     |
| ```delightql                  |                                     |
| genre_2024(*) as x |;|        |   INTERSECT ALL                     |
|   genre_2025(*) as y,         |                                     |
|   x.* = y.*                   |  (name safe)                        |
| ```                           |                                     |
+-------------------------------+-------------------------------------+
|                               |                                     |
| ```delightql                  |                                     |
| employee_2024(*) as x ||      |   INTERSECT ALL                     |
|   employee_2025(*) as y,      |                                     |
|   x|*| = y|*|                 |  (positional,                       |
| ```                           |    = SQL's `INTERSECT ALL`)         |
+-------------------------------+-------------------------------------+
|                               |                                     |
| ```delightql                  | Per-column intersection             |
| employee_2024(*) as x ||      |   (positional,                      |
|   employee_2025(*) as y,      |    no SQL equivalent)               |
|   x|1| = y|1|                 |                                     |
| ```                           |                                     |
+-------------------------------+-------------------------------------+
|                               |                                     |
| ```delightql                  | Per-column intersection             |
| genre_2024(*) as x |;|        |   (no SQL equivalent)               |
|   genre_2025(*) as y,         |                                     |
|   x.genre_id = y.genre_id     |                                     |
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
| ```delightql                  |   ```delightql                 |
|                               |                                |
| genre_2024(*) as g1,          |   genre_2024(*) as g1 |;|      |
|   genre_2025(*) as g2,        |     genre_2025(*) as g2,       |
|   g1.genre_id = g2.genre_id   |     g1.genre_id = g2.genre_id  |
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
> ```delightql
> customer(*) as c ; employee(*) as e,
>   c.city = e.city
> ```
>
> ```sql
> SELECT
>   customer_id, first_name, last_name, company, address, city,
>   state, country, postal_code, phone, fax, email, support_rep_id,
>   NULL, NULL, NULL, NULL, NULL
> FROM customer AS c
>   WHERE EXISTS (SELECT 1
>     FROM (
>       SELECT
>         employee_id, last_name, first_name, title, reports_to,
>         birth_date, hire_date, address, city, state, country,
>         postal_code, phone, fax, email
>       FROM employee AS e
>     ) AS t1
>     WHERE c.city IS NOT DISTINCT FROM t1.city)
>
> UNION ALL
>
> SELECT
>   NULL, first_name, last_name, NULL, address, city,
>   state, country, postal_code, phone, fax, email, NULL,
>   employee_id, title, reports_to, birth_date, hire_date
> FROM employee AS e
>   WHERE EXISTS (SELECT 1
>     FROM (
>       SELECT
>         customer_id, first_name, last_name, company, address,
>         city, state, country, postal_code, phone, fax, email,
>         support_rep_id
>       FROM customer AS c
>     ) AS t0
>     WHERE t0.city IS NOT DISTINCT FROM e.city)
> ```

