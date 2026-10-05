
# Tree Distinction {.dqlh}

Tree structures can serve as grouping columns, enabling aggregation alongside
hierarchical output:
```delightql
invoice(*)
  |> %( { billing_country,
          "invoices": ~> {invoice_id, invoice_date},
          billing_city } as invoices_by_country_and_city
          ~>
        sum:(total), count:(*) )
```


**Restriction:** Columns referenced in nested tree groups cannot also appear as
explicit grouping columns:

```{.delightql .bad}
// INVALID: invoice_date appears in tree group and as grouping column
invoice(*)
  |> %( { billing_country, "invoices": ~> {invoice_id, invoice_date}, billing_city } as tree,
        invoice_date
          ~>
        sum:(total) )
```

Columns not referenced in the tree may be added:
```delightql
invoice(*)
  |> %( { billing_country, "invoices": ~> {invoice_id, invoice_date}, billing_city } as tree,
        customer_id
          ~>
        sum:(total), count:(*) )
```

