
# Sigilization {.dqlh}

Delightql has few keywords compared to SQL.

Throughout this reference, the term *sigilization* describes delightql's practice
of representing operators with non-alphanumeric symbols rather than keywords.

For example, delightql sigilizes the **DISTINCT** relational operator with a `%` symbol
followed by parentheses:

```delightql
users(*) |> %(last_name)
// SQL: SELECT DISTINCT last_name FROM users
```

To continue this example, delightql recognizes that `GROUP BY` is simply `DISTINCT`
extended with aggregation. The same `%` sigil handles both  -- separate grouping
columns from aggregate functions with `~>` to get the equivalent of SQL's `GROUP
BY`:

```delightql
users(*) |> %(last_name ~> count:(*), sum:(salary) as salary_by_last_name)
// SQL: SELECT count(*), sum(salary) as salary_by_last_name FROM users GROUP BY last_name
```



