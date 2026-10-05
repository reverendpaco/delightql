# Argumentative Grounding {.dqlh}

When using argumentative functor notation, a ground term in argument position induces selection:

```delightql
album(album_id,title,22)
```

```sql
select
  album_id,
  title
from album where artist_id IS NOT DISTINCT FROM 22;
```

**All argumentative grounding uses null-safe equality**.

The grounded column (`artist_id  IS NOT DISTINCT FROM  22`) filters rows and is excluded from projection. Multiple grounds compound:

```delightql
album(album_id,"Coda",22)
```


```sql
SELECT album_id
FROM album
WHERE
  title IS NOT DISTINCT FROM 'Coda'
  AND artist_id IS NOT DISTINCT FROM 22;
```

Any domain expression that reduces to a ground term may also be used in argumentative position:

```delightql
album(album_id,upper:("iv"),(20 + 2))
```

```sql
select
  album_id
from album where title IS NOT DISTINCT FROM upper('iv') and artist_id IS NOT DISTINCT FROM (20+2);
```

The Prolog heritage is evident in this syntax and extends to joins -- covered in a later section.
