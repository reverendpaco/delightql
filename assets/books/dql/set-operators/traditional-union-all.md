
# Positional (Traditional) Union All (`||`{.delightql .sigil}) {.dqlh}

**Aligns by position**. Requires identical column count. Useful for intentional realignment.[The below
example uses interior relations to shape each relation prior to the `UNION
ALL`.]{.sidenote}:

```delightql
invoice(|> (billing_address,billing_city,billing_country))
  ||
customer(|> (address,city,country))
```
