# Interior Relations and Lateral Joins {.dqlh}

Interior relations unify `EXISTS`, `NOT EXISTS`, scalar subqueries, and lateral joins under one syntax.

An interior relation is a query continuation inside a functor's parentheses:

```dql
users(|> (last_name,first_name))
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

```dql
users(*) |> (last_name,first_name)
// equivalent to: users(|> (last_name,first_name))
```


Consider positional union all `||`{.delightql .sigil}
where interiority crafts the proper alignment and projection:

```delightql
users_2024(|> (last_name,first_name,age))
  ||
users_2023(|> (LastName,First,Age))
```


Interior relations are used in the following:

- scalar subqueries (regardless of correlation)
- `EXISTS` and `NOT EXISTS`
- simple shadowing subqueries
- correlated (non-scalar) subqueries -- i.e. lateral joins


## Scalar subqueries {.dqlh}

Scalar subqueries use interior notation:

```{.delightql .numberLines}
employee(*) as e
    |> (FirstName,
        LastName,
        Salary,
        employee:( ~> avg:(Salary)) as AvgSalary,
        employee:( , DepartmentName=e.DepartmentName
                   ~> avg:(Salary)) as AvgSalaryInDept)
```

In the above example, two query continuations started with `~>`{.delightql
.sigil} and `,`{.delightql .sigil} execute an uncorrelated and correlated
**scalar** subquery respectively.

## `EXISTS` and `NOT EXISTS` {.dqlh}


The `+`{.delightql .sigil} and `\+`{.delightql .sigil} prefixes with interior notation create `(NOT) EXISTS`:


```dql
users(*), orders(*),
  users.id = orders.user_id,
  \+order_items(, orders.id = order_items.order_id)
```


## Simple Shadowing {.dqlh}

Uncorrelated interior relations are simple shadowing -- useful for reshaping before set operations:

```dql
users(|> (last_name,first_name))
```

but especially for set operators:

```delightql
users_2024(|> (last_name,first_name,age))
  ||
users_2023(|> (LastName,First,Age))
```

```delightql
users_2024(|> *(last_name as LastName,first_name as First,age as Age))
  |;|
users_2023(*)
```

```delightql
users_2024(; users_2023(*)) as combined,
  org(*), combined.departments=org.dept
  |> (last_name,org.dept)
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
orders(*) ,
  users(, users.id=user_id
        |> (last_name,first_name,email)) as u
  |> (orders.*,last_name,first_name,email)
```

```sql
SELECT orders.*, last_name , first_name , email
  FROM (
  SELECT *
    FROM orders
  INNER JOIN (
    SELECT last_name , first_name , email ,
      id  -- promote out of subquery for joining
    FROM users
  ) AS users ON users.id IS NOT DISTINCT FROM user_id
);
```

**Aggregate Lateral**. Replaces multiple aggregate scalar subqueries.
Advantageous when the aggregate key matches the join key.

```dql
users(*) as u,
  orders(, orders.user_id = u.id |>
            %(user_id
              ~> sum:(total) as total_spent,
                 sum:(tax_amount) as total_tax_amount))
```

```sql
SELECT
  *
FROM users AS u
  INNER JOIN (
    SELECT user_id , sum(total) AS total_spent, sum(tax_amount) AS total_tax_amount
    FROM orders
    GROUP BY user_id
  ) AS orders
ON orders.user_id IS NOT DISTINCT FROM u.id;
```

**Top-N Lateral**. Returns the top N correlated rows per outer row, avoiding explicit window functions.

```dql
users(*) as u,
  orders(, orders.user_id = u.id |> #(total desc), #<3)
```

```sql
SELECT *
FROM users AS u
JOIN (SELECT
  id, order_id, user_id, customer_id, total,
  tax_amount, shipping_cost, status, created_at,
  shipped_at, delivered_at
FROM (SELECT
  id, order_id, user_id, customer_id, total,
  tax_amount, shipping_cost, status, created_at,
  shipped_at, delivered_at,
  ROW_NUMBER() OVER (
    PARTITION BY
      user_id
    ORDER BY total DESC
  ) AS __dql_rn
FROM orders) AS orders_with_rn
WHERE
  orders_with_rn.__dql_rn <= 3) AS orders
  ON orders.user_id IS NOT DISTINCT FROM u.id;
```

Note how the windowing function above partitions by the correlation
join condition.

## Summary {.dqlh}

The below diagram describes the hierarchy of
all places where delightql uses interiority.

Only the tree labeled `interior relations` consists
of expressions that are used as actual relations/tables.

```
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
