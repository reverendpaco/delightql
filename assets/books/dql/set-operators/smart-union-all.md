# Smart Union All (`|;|`{.delightql .sigil}) {.dqlh}

Aligns by name, but requires both relations to have identical column count and names. Unlike
SQL's `UNION/UNION-ALL` position is irrelevant. The resulting schema
is adopted from the first relation:

```delightql
genre_2024(*)
  |;|  genre_2025(*)
```


```sql
SELECT
  genre_id, name
FROM genre_2024
UNION ALL
SELECT
  genre_id, name  -- genre_2025 stores (name, genre_id)
FROM genre_2025;
```
