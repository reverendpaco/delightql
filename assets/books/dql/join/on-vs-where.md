
# ON vs WHERE Inference {.dqlh}

For inner joins, the placement of conditions in `ON` versus `WHERE` is
semantically equivalent. For outer joins, it differs: `ON` conditions govern
the match while preserving nulls; `WHERE` conditions filter the result and
eliminate nulls.

Delightql infers placement from column references:

- **Condition references multiple tables** → `ON`
- **Condition references one table** → `WHERE`
```delightql
artist(*), album?(*),
  artist.artist_id = album.artist_id,   // two tables → ON
  album.title = "Greatest Hits"       // one table → WHERE
```
```sql
SELECT * FROM artist
  LEFT OUTER JOIN album
    ON artist.artist_id = album.artist_id
WHERE album.title = 'Greatest Hits';
```

The multi-table condition (`artist.artist_id = album.artist_id`) becomes the
join's `ON` clause. The single-table condition (`album.title = 'Greatest Hits'`)
becomes a `WHERE` filter -- artists with no albums, or with no album of that
title, are excluded.


**TODO**:  ensure that named sigma-rules also obey this rule.
