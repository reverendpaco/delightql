# Tables Column Modality {.dqlh}

Any syntax of the form `foo(x,y)` means that the columns may be input (supplied) or
output (returned).

This matches the syntax and semantics of Prolog.

```delightql
genre(id,"Jazz")
```

In the above form, the `id` variable is output
and the `"Jazz"` argument is input, producing a WHERE
predication via grounding:

```sql
select genre_id as id from genre where name = 'Jazz';
```

It is important to understand that in Prolog-like argumentatitve join syntax
both columns are input at the same time :

```delightql
playlist_track(5,track),playlist_track(17,track)
```

In the above the variable `track` is input (or bidirectionally unified) to both predicates,
creating a join.

The GLOB makes all columns into output columns:

```delightql
genre(*)
```

without having to enumerate all dimension positions.
