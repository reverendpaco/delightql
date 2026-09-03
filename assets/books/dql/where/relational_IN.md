
# Relational `in` {.dqlh}

The literal form tests membership in a fixed list. The relational form tests
membership in the result of a query -- SQL's `IN (SELECT ...)`.

The right-hand side is any DQL relation (a table access, a pipe chain, or an
anonymous table):

```delightql
employee(*), DepartmentId in department(|> (DepartmentId))
```

```sql
SELECT * FROM employee
  WHERE DepartmentId IN (SELECT DepartmentId FROM department);
```

When the relation already has exactly one column, projection is unnecessary:

```delightql
employee(*), State in valid_states(*)
```

```sql
SELECT * FROM employee
  WHERE State IN (SELECT State FROM valid_states);
```



## Tuple relational `in` {.dqlh}

Multi-column matching extends the tuple `in` syntax (`(x,y) in (1,2;3,4)`)
to relations. The relation must produce exactly as many columns as the
left-hand tuple:

```delightql
employee(*), (State, Department) in valid_combos(|> (State, Department))
```

```sql
SELECT * FROM employee
  WHERE (State, Department) IN
    (SELECT State, Department FROM valid_combos);
```


### Negation: `not in` {.dqlh}

```delightql
employee(*), DepartmentId not in terminated_depts(|> (DepartmentId))
```

```sql
SELECT * FROM employee
  WHERE DepartmentId NOT IN (SELECT DepartmentId FROM terminated_depts);
```


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

