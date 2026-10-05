
# Argumentative Join {.dqlh}

Shared identifiers across functors induce join conditions -- Prolog-style unification:
```delightql
album(album_id, title, artist_id), artist(artist_id, name)
```
```sql
SELECT album.album_id, album.title, album.artist_id, artist.name
FROM album
  JOIN artist ON album.artist_id = artist.artist_id;
```

The variable `artist_id` appears in both functors, unifying the columns.

Multi-table example:
```delightql
artist(artist_id, name),
  similar_artist(artist_id, similar_id, score),
  artist(similar_id, similar_name),
  score > 0.85
  |> (name, similar_name)
```

Argumentative joins are idiomatic in Prolog. Delightql supports them but
recommends `.(cols)` or explicit conditions for wide tables where positional
notation becomes error-prone.


