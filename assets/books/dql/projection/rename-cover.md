# Rename Cover {.dqlh}

The RENAME-COVER operator `*(  )`{.delightql .sigil} renames specified columns while passing all others through:

```delightql
track(*)
  |> *( name as track_name)
```

```sql
select
    track_id,
    name as track_name,
    album_id,
    media_type_id,
    genre_id,
    composer,
    milliseconds,
    bytes,
    unit_price
from track;
```

Rename cover preserves column count and column ordinality -- only the names change.
