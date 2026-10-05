
# Stacked Notation (Named Case) {.dqlh}

The stacked form defines functions as lookup tables with explicit input-output mappings:
```{.delightql .am}
title_kind(
  title          -> kind
  ------------------
  "IT Manager"   -> "tech";
  "IT Staff"     -> "tech";
  _              -> "other"
)
```

The `->` separates inputs (left) from outputs (right). The header row names the columns; subsequent rows provide the mappings. The `_` matches any input not explicitly listed.

Despite the visual similarity to anonymous table stacked notation, this is an assertion-mode construct -- it defines a reusable function, not inline data.

**Invocation:**
```delightql
employee(*) |> +(title_kind:(title) as kind)
```
```sql
SELECT *,
  CASE title
    WHEN 'IT Manager' THEN 'tech'
    WHEN 'IT Staff' THEN 'tech'
    ELSE 'other'
  END AS kind
FROM employee;
```

**Multi-column inputs:**
```{.delightql .am}
tax_rate(
  country, state -> rate
  --------------------------
  "USA", "CA"    -> 0.0725;
  "USA", "TX"    -> 0.0625;
  "Canada", "ON" -> 0.13;
  "Canada", "AB" -> 0.05;
  _              -> 0.0
)
```
```delightql
invoice(*) |> +(tax_rate:(billing_country, billing_state) as tax)
```
