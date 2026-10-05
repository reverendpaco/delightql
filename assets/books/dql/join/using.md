
# `USING` Shorthand {.dqlh}

The `.(cols)` operator specifies USING columns:
```delightql
album(*), artist(*.(artist_id))
```
```sql
SELECT * FROM album JOIN artist USING (artist_id);
```

Multiple columns are comma-separated:
```delightql
invoice_line(*), track(*.(track_id, unit_price))
```
```sql
SELECT * FROM invoice_line JOIN track USING (track_id, unit_price);
```

`USING` and explicit `ON` differ operationally: `USING` retains one copy of the matched column; `ON` retains both.

`USING` may combine with explicit conditions:
```delightql
track(*), album(*.(album_id)), track.name = album.title
```


