# Distinct {.dqlh}

The GROUP-MODULO operator `%(  )`{.delightql .sigil}
returns distinct combinations of the specified columns:

```delightql
customer(*)
  |> %(country)
```


```sql
select
  distinct country
from customer;
```

Multiple columns return distinct combinations:

```delightql
customer(*)
  |> %(country, state)
  |> #(country,state descending)
```

```sql
select
  distinct country, state
from customer
    order by country asc, state desc;
```

To deduplicate all columns -- converting a multiset (bag) into a set:

```delightql
genre_2025(*)
  |> %(*)  //returns unique rows and removes duplicates
```
