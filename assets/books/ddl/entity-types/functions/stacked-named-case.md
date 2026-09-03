
# Stacked Notation (Named Case) {.dqlh}

The stacked form defines functions as lookup tables with explicit input-output mappings:
```delightql
department_kind(
  department     -> kind
  ------------------
  "engineering"  -> "tech";
  "data science" -> "tech";
  _              -> "other"
)
```

The `->` separates inputs (left) from outputs (right). The header row names the columns; subsequent rows provide the mappings. The `_` matches any input not explicitly listed.

Despite the visual similarity to anonymous table stacked notation, this is an assertion-mode construct -- it defines a reusable function, not inline data.

**Invocation:**
```delightql
employee(*) |> +(department_kind:(Department) as kind)
```
```sql
SELECT *,
  CASE Department
    WHEN 'engineering' THEN 'tech'
    WHEN 'data science' THEN 'tech'
    ELSE 'other'
  END AS kind
FROM employee;
```

**Multi-column inputs:**
```delightql
tax_rate(
  state, category -> rate
  --------------------------
  "CA", "food"    -> 0.0;
  "CA", "electronics" -> 0.0825;
  "TX", "food"    -> 0.0;
  "TX", "electronics" -> 0.0625;
  _, _            -> 0.05
)
```
```delightql
products(*) |> +(tax_rate:(state, category) as tax)
```
