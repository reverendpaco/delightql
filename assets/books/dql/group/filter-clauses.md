# Filter Clauses {.dqlh}

The IF-ONLY sigil `|`{.delightql .sigil} constrains which values enter an aggregate:

```delightql
customer(*)
  |>  %( country ~>
         count:(%city) ,
         count:(%support_rep_id),
         count:(last_name | length:(last_name) > 7)
            as long_lastname_count)
```

For dialects supporting `FILTER`:

```sql
select
  country,
  count(distinct city),
  count(distinct support_rep_id),
  count(last_name)
    filter
      (where length(last_name) > 7) as long_lastname_count
from customer
  group by country;
```

For dialects without `FILTER`, delightql emits a `CASE` expression:

```sql
select
  country,
  count(distinct city),
  count(distinct support_rep_id),
  count(case when length(last_name) > 7
            then last_name else null) as  long_lastname_count
from customer
  group by country;
```
