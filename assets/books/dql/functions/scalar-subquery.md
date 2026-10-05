# Scalar Subquery {.dqlh}

Scalar subqueries return a single value (one row, one column) usable anywhere a
column is valid. Delightql transforms a relation into a scalar subquery by
postfixing its name with `:`{.delightql .sigil} and using interior notation.

**Uncorrelated**. The subquery is independent of the outer query:

```{.delightql .numberLines}
invoice(*)
    |> (invoice_id,
        billing_country,
        total,
        invoice:( ~> avg:(total)) as avg_total)
```

```sql
select
  invoice_id,
  billing_country,
  total,
  (select avg(total) from invoice) as avg_total
from invoice;
```

The F-COLON sigil `:`{.delightql .sigil} after the relation name signals a scalar subquery. The
interior notation--here `~> avg:(total)`{.delightql} -- must produce exactly one row and one
column.

**Correlated**. The subquery references values from the outer query. Use an explicit condition to correlate on a column:

:::::{.widen}
`tpt:#numbering_on()`
```{.delightql .numberLines}
invoice(*) as i
    |> (invoice_id,
        billing_country,
        total,
        invoice:( ~> avg:(total)) as avg_total,
        invoice:( , billing_country = i.billing_country ~> avg:(total)) as avg_total_in_country)
```
`tpt:#numbering_off()`
:::::::


```sql
select
  invoice_id,
  billing_country,
  total,
  (select avg(total) from invoice) as avg_total,
  (select avg(total) from invoice
      where billing_country = i.billing_country) as avg_total_in_country
from invoice i;
```
