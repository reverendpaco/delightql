
# Null Semantics {.dqlh}

## Equality expressions {.dqlh}

When both sides of a delightql `=` expression come from two different relations, then the traditional SQL `=` is used during transpilation. This rule applies
for regular joins and inner relations (EXISTS and scalar subqueries).

When the above condition is not met, then delightql transpiles `=` to SQL's
`IS NOT DISTINCT FROM`.

## Unification equality {.dqlh}

The rules of equality expressions are created to use null-safe equality ()
in all places where join semantics are at play.  This applies to
Prolog-style unification as well:

```delightql
playlist(playlist_id, name), playlist_track(playlist_id, track_id)
```

```sql
SELECT playlist.playlist_id, playlist.name, playlist_track.track_id
FROM playlist, playlist_track
WHERE playlist.playlist_id = playlist_track.playlist_id;
```


```delightql
employee(*) as e, employee(*) as m, e.reports_to = m.employee_id
```


```sql
SELECT *
FROM employee e, employee m
WHERE e.reports_to = m.employee_id;
```

## Traditional SQL Equals {.dqlh}


If you must transpile the regular SQL equals in non-join
locations, you can use `+sql_eq(l,r)` which is included
as part of `std::prelude`.
