
# Cross Join {.dqlh}

```delightql
media_type(*), genre(*)
```

```sql
SELECT * FROM media_type CROSS JOIN genre;
```

Two relations joined with no condition produce a Cartesian product. The
resulting cardinality is the product of both input cardinalities. This is
rarely intended.
