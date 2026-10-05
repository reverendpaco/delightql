# Common Table Expressions {.dqlh}

Create a common table expression (**CTE**) by naming a functor with a glob, followed by a
`:`{.delightql .sigil}, also known as **SHADOW-NECK**, followed by the
query that is assigned to that name. This syntax is called *pre-labeling*.

Once, a common table expression is defined, it is sufficient to query
from that as if it were a table.

```delightql
big_invoices(*) : invoice(*), total > 15
big_invoices(*)
```


```sql
WITH "big_invoices" AS (
  SELECT *
  FROM "invoice"
  WHERE "total" > 15
)
SELECT *
FROM "big_invoices";
```

An alternate syntax, called *post-labeling*, allows the CTE to be named
after the query by postfixing a valid query with the **SHADOW-NECK** `:`{.delightql}
and a simple identifier:

```delightql
invoice(*), total > 15 : big_invoices
big_invoices(*)
```

> Note: post-labeling can only be used on lower-order relational predicates.
> Common function expressions and higher-order CTEs must be  pre-labeled.

These syntaxes may be intermixed:

```delightql
us_customers(*): customer(*), country = "USA"
invoice(*), total > 15 : big_invoices
us_customers(*), big_invoices(*.(customer_id))
```

```sql
WITH "us_customers" AS (
  SELECT *
  FROM "customer"
  WHERE "country" IS NOT DISTINCT FROM 'USA'
),
"big_invoices" AS (
  SELECT *
  FROM "invoice"
  WHERE "total" > 15
)
SELECT *
FROM "us_customers"
JOIN "big_invoices" USING ("customer_id");
```
