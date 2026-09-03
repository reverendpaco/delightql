# Common Table Expressions {.dqlh}

Create a common table expression (**CTE**) by naming a functor with a glob, followed by a
`:`{.delightql .sigil}, also known as **SHADOW-NECK**, followed by the
query that is assigned to that name. This syntax is called *pre-labeling*.

Once, a common table expression is defined, it is sufficient to query
from that as if it were a table.

```delightql
adults(*) : users(*), age > 30
adults(*)
```


```sql
WITH "adults" AS (
  SELECT *
  FROM "users"
  WHERE "age" > 30
)
SELECT *
FROM "adults";
```

An alternate syntax, called *post-labeling*, allows the CTE to be named
after the query by postfixing a valid query with the **SHADOW-NECK** `:`{.delightql}
and a simple identifier:

```delightql
users(*), age > 30 : adults
adults(*)
```

> Note: post-labeling can only be used on lower-order relational predicates.
> Common function expressions and higher-order CTEs must be  pre-labeled.

These syntaxes may be intermixed:

```delightql
us_users(*): users(*), country = 'USA'
orders(*), status = 'completed' : completed_orders
us_users(*), completed_orders(*)
```

```sql
WITH "us_users" AS (
  SELECT *
  FROM "users"
  WHERE "country" IS NOT DISTINCT FROM 'USA'
),
"completed_orders" AS (
  SELECT *
  FROM "orders"
  WHERE "status" IS NOT DISTINCT FROM 'completed'
)
SELECT *
FROM "us_users"
CROSS JOIN "completed_orders";
```
