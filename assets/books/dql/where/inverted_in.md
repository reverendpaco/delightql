
# Inverted `In` {.dqlh}

The anonymous semi-join syntax permits an inversion -- ground the header, vary the rows:


```delightql
customer(*),
    +_("Dublin" @
      city;
      state;
      country)
```


```sql
select
  *
from customer
  where city = 'Dublin'
    or state = 'Dublin'
    or country = 'Dublin';
```


This asks: "does 'Dublin' appear in any of these columns?" The columns become the
rows of the anonymous table; the constant becomes the match target.
Because `'Dublin'` is non-NULL, SQL `=` is a result-equivalent filter here.
Grounding a candidate's header to NULL is instead a local null-safe test.

>   **SQL supports Inverted In**
>
>  Though it might be a revelation to some -- including the author! --
>  the inverted in is standard SQL and is fully supported by all dialects:
>
>   ```sql
>   select
>     *
>   from customer
>     where 'Dublin' in
>       (city,state,country);
>   ```


Similarly, to test if one column equals any of several others:


```delightql
customer(*), +_(city @ state; country)
```

```sql
select
  *
from customer
  where city = state
    or city = country;
```

Here `city` is supplied by the customer row and each candidate is
an anonymous-table row. `+` keeps or drops the customer depending on whether
such a row corresponds; it does not turn NULL-to-NULL into a match. A
wholly ground anonymous test still follows the local ground-value law.
