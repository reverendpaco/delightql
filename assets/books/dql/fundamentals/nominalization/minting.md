
# Minting {.dqlh}

Any column that results from a function application
or from a column ambiguity is automatically minted so that the column name may never be used again.

## Function Application Minting {.dqlh}

```delightql
invoice_line(*)
  |> +( unit_price * quantity)
```

The `unit_price * quantity` column has a name generated for it that may never be used again. This means that absent naming the column at creation with `as` the only means of accessing this column is by ordinal index.

```delightql
invoice_line(*)
  |> +( unit_price * quantity)
  |> ( |-1| )
```

## Column Ambiguity {.dqlh}

```delightql
track(*), genre(*.(genre_id))
  |> (*)
  // Track.Name and Genre.Name receive
  // minted names
```

If `track.name` and `genre.name` are in-scope in the above example, then both are minted as neither are more real than the other.

Solve this by removing or renaming one of the columns:

```delightql
track(*), genre(*.(genre_id))
  |> -( genre.name)
  // Name is addressable now
```


```delightql
track(*), genre(*.(genre_id))
  |> *( genre.name as genre_name)
  // Name is addressable now
  // genre_name is addressable now
```
