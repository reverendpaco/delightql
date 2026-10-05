
# Sigilization {.dqlh}

Delightql has few keywords compared to SQL.

Throughout this reference, the term *sigilization* describes delightql's practice
of representing operators with non-alphanumeric symbols rather than keywords.

For example, delightql sigilizes the **DISTINCT** relational operator with a `%` symbol
followed by parentheses:

```delightql
invoice(*) |> %(billing_country)
// SQL: SELECT DISTINCT billing_country FROM Invoice
```

To continue this example, delightql recognizes that `GROUP BY` is simply `DISTINCT`
extended with aggregation. The same `%` sigil handles both  -- separate grouping
columns from aggregate functions with `~>` to get the equivalent of SQL's `GROUP
BY`:

```delightql
invoice(*) |> %(billing_country ~> count:(*), sum:(total) as total_by_country)
// SQL: SELECT billing_country, count(*), sum(Total) as total_by_country FROM Invoice GROUP BY billing_country
```



