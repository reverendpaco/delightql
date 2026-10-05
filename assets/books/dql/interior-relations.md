# Interior Relations and Lateral Joins {.dqlh}

Interior relations unify `EXISTS`, `NOT EXISTS`, scalar subqueries, and lateral joins under one syntax.

An interior relation is a query continuation inside a functor's parentheses:

```delightql
customer(|> (last_name,first_name))
```

> **Query Continuation**
>
> A query continuation extends a complete query rightward.
>
> `users(*)‸ , age<50‸ |> (department)‸`{.delightql}
>
> After `users(*)`{.delightql}, the continuation `, age>50 |> (department)`{.delightql}
> is valid because `users(*)`{.delightql} alone is
> already meaningful. Likewise, `|> (department)`{.delightql} is valid because
> `users(*), age>50`{.delightql} is already meaningful.
>

Interior relations appear wherever tables are allowed. When uncorrelated,
they're equivalent to exterior execution:

```delightql
customer(*) |> (last_name,first_name)
// equivalent to: Customer(|> (last_name,first_name))
```


Consider positional union all `||`{.delightql .sigil}
where interiority crafts the proper alignment and projection:

```delightql
customer(|> (first_name,last_name,company))
  ||
employee(|> (first_name,last_name,title))
```


Interior relations are used in the following:

- scalar subqueries (regardless of correlation)
- `EXISTS` and `NOT EXISTS`
- simple shadowing subqueries
- correlated (non-scalar) subqueries -- i.e. lateral joins


## Scalar subqueries {.dqlh}

Scalar subqueries use interior notation:

```{.delightql .numberLines}
invoice(*) as i
    |> (invoice_id,
        billing_country,
        total,
        invoice:( ~> avg:(total)) as avg_total,
        invoice:( , billing_country=i.billing_country
                  ~> avg:(total)) as avg_total_in_country)
```

In the above example, two query continuations started with `~>`{.delightql
.sigil} and `,`{.delightql .sigil} execute an uncorrelated and correlated
**scalar** subquery respectively.

## `EXISTS` and `NOT EXISTS` {.dqlh}


The `+`{.delightql .sigil} and `\+`{.delightql .sigil} prefixes with interior notation create `(NOT) EXISTS`:


```delightql
album(*), track(*),
  album.album_id = track.album_id,
  \+invoice_line(, track.track_id = invoice_line.track_id)
```

The `EXISTS` observer asks whether a row matched; it does not change the
matching condition. A comparison between an inner and an enclosing row
uses correspondence equality (`=` in SQL), including when the same
interior is observed as a scalar subquery. A local test within the inner
row keeps its ordinary null-safe meaning.


## Simple Shadowing {.dqlh}

Uncorrelated interior relations are simple shadowing -- useful for reshaping before set operations:

```delightql
customer(|> (last_name,first_name))
```

but especially for set operators:

```delightql
customer(|> (first_name,last_name,company))
  ||
employee(|> (first_name,last_name,title))
```

```delightql
employee_2025(|> *(job_title as title,manager_id as reports_to))
  |;|
employee_2024(*)
```

```delightql
employee_2024(; employee_2025(*)) as staff,
  customer(*), staff.employee_id=customer.support_rep_id
  |> (staff.last_name,customer.email)
```

## Correlated Table (Lateral Join) {.dqlh}

Any table with interiority and correlation to other tables **in the
same query** is a lateral join.

Lateral joins may be broken down into three sub-types:

- simple
- aggregate
- top-N


**Simple Lateral**. A join without aggregation or limits. Replaces multiple scalar
subqueries; rarely advantageous over a regular join.

```delightql
invoice(*) ,
  customer(, c.customer_id=invoice.customer_id
        |> (last_name,first_name,email)) as c
  |> (invoice.*,last_name,first_name,email)
```

```sql
SELECT invoice.*, last_name , first_name , email
  FROM invoice
  INNER JOIN (
    SELECT last_name , first_name , email ,
      customer_id  -- promote out of subquery for joining
    FROM customer
  ) AS c ON c.customer_id = invoice.customer_id;
```

**Aggregate Lateral**. Replaces multiple aggregate scalar subqueries.
Advantageous when the aggregate key matches the join key.

```delightql
customer(*) as c,
  invoice(, invoice.customer_id = c.customer_id |>
            %(customer_id
              ~> sum:(total) as total_spent,
                 count:(*) as invoice_count))
```

```sql
SELECT
  *
FROM customer AS c
  INNER JOIN (
    SELECT customer_id , sum(total) AS total_spent, count(*) AS invoice_count
    FROM invoice
    GROUP BY customer_id
  ) AS invoice
ON invoice.customer_id = c.customer_id;
```

**Top-N Lateral**. Returns the top N correlated rows per outer row, avoiding explicit window functions.

```delightql
customer(*) as c,
  invoice(, invoice.customer_id = c.customer_id |> #(total desc), #<3)
```

```sql
SELECT *
FROM customer AS c
JOIN (SELECT
  invoice_id, customer_id, invoice_date, billing_address,
  billing_city, billing_state, billing_country,
  billing_postal_code, total
FROM (SELECT
  invoice_id, customer_id, invoice_date, billing_address,
  billing_city, billing_state, billing_country,
  billing_postal_code, total,
  ROW_NUMBER() OVER (
    PARTITION BY
      customer_id
    ORDER BY total DESC
  ) AS __dql_rn
FROM invoice) AS invoices_with_rn
WHERE
  invoices_with_rn.__dql_rn <= 3) AS invoice
  ON invoice.customer_id = c.customer_id;
```

Note how the windowing function above partitions by the correlation
join condition.

## Summary {.dqlh}

The below diagram describes the hierarchy of
all places where delightql uses interiority.

Only the tree labeled `interior relations` consists
of expressions that are used as actual relations/tables.

```text
  Interiority:
  ├── interior relations:
  │   ├── correlated (lateral):
  │   │   ├── top-N
  │   │   ├── simple/multi
  │   │   └── aggregate
  │   └── uncorrelated
  ├── (not) exists
  └── scalar subqueries
      ├── correlated
      └── uncorrelated
```
