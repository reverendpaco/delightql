
# Set Semantics vs Multiset Semantics {.dqlh}

All of delightql set operators are actually
multiset operators inasmuch as they preserve duplicates.
In other words, all set operators are `ALL`-flavored.

If set semantics are required, use `DISTINCT ALL` via `|> %(*)`{.delightql}.

```delightql
genre_2024(*) |;| genre_2025(*) |> %(*)
```

```sql
SELECT
  genre_id, name
FROM genre_2024
  UNION  --- NOT UNION ALL
SELECT
  genre_id, name
FROM genre_2025;
```

which is equivalent to

```sql
SELECT DISTINCT * FROM
  (SELECT
    genre_id, name
  FROM genre_2024
    UNION ALL
  SELECT
    genre_id, name
  FROM genre_2025)
;
```

