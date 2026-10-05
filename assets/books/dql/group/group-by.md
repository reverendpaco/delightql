# Group By {.dqlh}

`Group by` extends distinct with aggregation. The AGG-AND sigil `~>`{.delightql
.sigil} separates grouping columns (left) from reduced columns (right):

```delightql
invoice(*)
  |> %(billing_country ~>  count:(*) , sum:(total) )
```



```sql
select
  billing_country,  -- grouping column
  count(*),        -- reduced column
  sum(total)       -- reduced column
from invoice
  group by billing_country;
```

Grouping columns may be expressions:

```delightql
invoice(*)
    |> %( total > 10  as high_low,
          upper:(billing_country) ~>
            count:(*) ,
            avg:(total) )
```


```sql
select
  total > 10 as high_low, -- grouping column
  upper(billing_country),  -- grouping column
  count(*),    -- reduced column
  avg(total)   -- reduced column
from invoice
  group by upper(billing_country), (total > 10) ;
```
