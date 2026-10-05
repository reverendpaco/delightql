
# Relational `in` {.dqlh}

The literal form tests candidates in a fixed list. The relational form tests
candidate rows from a query. Both are existence observations over the
ordinary row-correspondence match, not a promise to emit SQL's
three-valued `IN (SELECT ...)`.

The right-hand side is any DQL relation (a table access, a pipe chain, or an
anonymous table):

```delightql
customer(*), support_rep_id in employee(|> (employee_id))
```

```sql
SELECT * FROM customer
  WHERE EXISTS (SELECT 1 FROM employee AS e
                WHERE e.employee_id = customer.support_rep_id);
```

When the relation already has exactly one column, projection is unnecessary:

```delightql
customer(*) |> %(support_rep_id) : support_rep
employee(*), employee_id in support_rep(*)
```

```sql
WITH support_rep AS (SELECT DISTINCT support_rep_id FROM customer)
SELECT * FROM employee
  WHERE EXISTS (SELECT 1 FROM support_rep AS r
                WHERE r.support_rep_id = employee.employee_id);
```



## Tuple relational `in` {.dqlh}

Multi-column matching extends the tuple `in` syntax (`(x,y) in (1,2;3,4)`)
to relations. The relation must produce exactly as many columns as the
left-hand tuple:

```delightql
customer(*), (city, country) in employee(|> (city, country))
```

```sql
SELECT * FROM customer
  WHERE EXISTS (SELECT 1 FROM employee AS e
                WHERE e.city = customer.city
                  AND e.country = customer.country);
```


### Negation: `not in` {.dqlh}

```delightql
employee(*), employee_id not in employee(|> (reports_to))
```

```sql
SELECT * FROM employee
  WHERE NOT EXISTS (SELECT 1 FROM employee AS m
                    WHERE m.reports_to = employee.employee_id);
```

The negative form is anti-existence, not SQL `NOT IN`: a NULL candidate
does not poison a nonmatching probe. A NULL employee key corresponds to
no candidate key, including another NULL; positive `in` then fails and
negative `not in` succeeds for that row. A wholly ground test such as
`null in (null)` is a local null-safe value comparison and succeeds.


> **Arity rule**
>
> The relation must produce exactly as many columns as the left side has
> elements -- one for a scalar, *N* for an *N*-tuple.  A mismatch is a
> compile-time error.

> **Relation to semi-joins**
>
> Relational `in` is syntactic sugar over the semi-join notation introduced
> [above](#semi-joins-and-anti-joins).
> `col in R(|> (c))` desugars to `+R(, col = c)`;
> `col not in R(|> (c))` desugars to `\+R(, col = c)`.
