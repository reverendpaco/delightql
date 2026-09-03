
# Cross Join {.dqlh}

```delightql
employee(*), department(*)
```

```sql
SELECT * FROM employee CROSS JOIN department;
```

Two relations joined with no condition produce a Cartesian product. The
resulting cardinality is the product of both input cardinalities. This is
rarely intended.
