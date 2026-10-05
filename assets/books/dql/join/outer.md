
# Outer Joins {.dqlh}

The OUTER-IND sigil `?`{.delightql .sigil} marks a relation as optional -- it
may contribute nulls when no match exists.

**Left outer** (right table optional):
```delightql
artist(*), album?(*.(artist_id))
```
```sql
SELECT * FROM artist LEFT OUTER JOIN album USING (artist_id);
```

**Right outer** (left table optional):
```delightql
album?(*), artist(*.(artist_id))
```

**Full outer** (either optional):
```delightql
album?(*), artist?(*.(artist_id))
```

Outer joins work with explicit conditions:
```delightql
artist(*), album?(*), artist.artist_id = album.artist_id
```
```sql
SELECT * FROM artist
  LEFT OUTER JOIN album ON artist.artist_id = album.artist_id;
```

