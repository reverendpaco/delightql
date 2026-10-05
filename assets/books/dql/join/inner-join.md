
# Inner Join {.dqlh}

```delightql
album(*), artist(*), album.artist_id = artist.artist_id
```

```sql
SELECT * FROM album
  JOIN artist ON album.artist_id = artist.artist_id;
```

The join condition follows the tables it correlates. Multiple conditions conjoin naturally:

```delightql
customer(*), employee(*),
  customer.support_rep_id = employee.employee_id,
  customer.country = employee.country
```

**Scope is left to right.** This is an error:
```{.delightql .bad}
// INVALID: Artist not yet in scope
album(*), artist.artist_id = album.artist_id, artist(*)
```

