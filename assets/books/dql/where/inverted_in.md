
# Inverted `In` {.dqlh}

The anonymous semi-join syntax permits an inversion -- ground the header, vary the rows:


```delightql
people(*),
    +_("MA" @
      birth_state;
      death_state;
      work_state;
      marriage_state)
```


```sql
select
  *
from people
  where birth_state IS NOT DISTINCT FROM 'MA'
    or death_state IS NOT DISTINCT FROM 'MA'
    or work_state IS NOT DISTINCT FROM 'MA'
    or marriage_state IS NOT DISTINCT FROM 'MA';
```


This asks: "does 'MA' appear in any of these columns?" The columns become the
rows of the anonymous table; the constant becomes the match target.

>   **SQL supports Inverted In**
>
>  Though it might be a revelation to some -- including the author! --
>  the inverted in is standard SQL and is fully supported by all dialects:
>
>   ```sql
>   select
>     *
>   from people
>     where 'MA' in
>       (birth_state,death_state,work_state,marriage_state);
>   ```


Similarly, to test if one column equals any of several others:


```delightql
people(*), +_(birth_state @ death_state; work_state; marriage_state)
```

```sql
select
  *
from people
  where birth_state IS NOT DISTINCT FROM death_state
    or birth_state IS NOT DISTINCT FROM work_state
    or birth_state IS NOT DISTINCT FROM marriage_state;
```

