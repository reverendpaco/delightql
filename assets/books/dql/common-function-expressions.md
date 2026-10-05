
# Common Function Expressions {.dqlh}

Create common function expressions (**CFEs**) -- functions whose name is
created for the duration of the query -- by *pre-labeling* with a *functional
functor*. [A functional functor is a functor with a colon separating the
identifier from the opening parenthesis.]{.sidenote} The **SHADOW-NECK**
separates the functional functor on the left from any valid domain expression
on the right. CFEs may **only** be created by pre-labeling.

```delightql
enweirden:(total) :
  total >> :(@ - 5) >> max:(0) >> min:(10)

invoice(*) |> (enweirden:(total) as silly, total)
```

```sql
SELECT
  min(max("total" - 5, 0), 10) AS "silly",
  "total" AS "total"
FROM "invoice";
```



CTEs and CFEs may be intermixed:

```delightql
double:(x) : (x * 2)
invoice(*), total > 5 : over_five
triple:(y) : (y * 3)
mid_range(*): over_five(*), total < 10
mid_range(*)
  |> ( invoice_id,
      billing_city,
      total,
      double:(total) as doubled,
      total >> double:() >> double:() as quadrupled,
      triple:(total) as tripled,
      double:(triple:(total)) as sextupled)
```

```sql
WITH over_five AS (
    SELECT
        *
    FROM invoice
    WHERE total > 5
),
mid_range AS (
    SELECT
        *
    FROM over_five
    WHERE total < 10
)
SELECT
    invoice_id,
    billing_city,
    total,
    (total * 2) AS doubled,
    ((total * 2) * 2) AS quadrupled,
    (total * 3) AS tripled,
    ((total * 3) * 2) AS sextupled
FROM
    mid_range;
```

## Clause-local parameters

A value function's clauses bind actuals by argument position. Each clause
chooses its own scalar names; arity, roles and context capture must agree.
This applies to common functions and consulted functions alike.

```delightql
f:(x | x > 1) : x * 10
f:(y) : y + 100
_(v @ 1; 3) |> (f:(v) as r)
```

The results are 101 and 30. The guard and computation in each clause use
that clause's names. Scalar parameter metadata identifies argument positions.
