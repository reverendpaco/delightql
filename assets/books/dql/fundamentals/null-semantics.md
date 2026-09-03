
# Null Semantics {.dqlh}

Delightql defaults to null-safety by choosing `=` to mean `IS NOT DISTINCT FROM`
in *most* situations.

| Sigil | Name                    | SQL Equivalent         |
|-------|-------------------------|------------------------|
| `=`   | **NULL-SAFE-GROUND-EQ** | `IS NOT DISTINCT FROM` |
| `!=`  | **NULL-SAFE-NOT-EQ**    | `IS DISTINCT FROM`     |
: Infix domain predicates

In joins importantly the `=` transpiles to the traditional SQL equality,
preventing NULL from matching with NULL and safeguarding
against cartesian explosion.


```delightql
users(user_id, name, _), orders(order_id, user_id, total, _)
```

```sql
SELECT users.name, orders.order_id, orders.total
FROM users, orders
WHERE users.user_id = orders.user_id;
```


```delightql
users(*), orders(*), users.user_id = orders.user_id
```


```sql
SELECT *
FROM users, orders
WHERE users.user_id = orders.user_id;
```

## Traditional SQL Equals {.dqlh}


If you must transpile the regular SQL equals in non-join
locations, you can use `+sql_eq(l,r)` which is included
as part of `std::prelude`.
