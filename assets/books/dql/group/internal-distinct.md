# Internal Distinct {.dqlh}

Some aggregates accept a distinct modifier on their input. The INNER-MODULO sigil `%`{.delightql .sigil} prefixes the column:

```{.delightql .numberLines}
customer(*)
    |>  %( country ~>
            count:(%city) ,
            count:(%support_rep_id))
```


```sql
select
  country,
  count(distinct city),
  count(distinct support_rep_id)
from customer
  group by country;
```
