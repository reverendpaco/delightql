
# Minting {.dqlh}

Any column that results from a function application
or from a column ambiguity is automatically minted so that the column name may never be used again.

## Function Application Minting {.dqlh}

```delightql
users(*)
  |> +( age + 2)
```

The `age + 2` column has a name generated for it that may never be used again. This means that absent naming the column at creation with `as` the only means of accessing this column is by ordinal index.

```delightql
users(*)
  |> +( age + 2)
  |> ( |-1| )
```

## Column Ambiguity {.dqlh}

```delightql
users(*), orders(*)
  |> (*)
  // users.id and orders.id receive
  // minted names
```

If `users.id` and `orders.id` are in-scope in the above example, then both are minted as neither are more real than the other.

Solve this by removing or renaming one of the columns:

```delightql
users(*), orders(*)
  |> -( orders.id)
  // id is addressable now
```


```delightql
users(*), orders(*)
  |> *( orders.id as order_id)
  // id is addressable now
  // order_id is addressable now
```
