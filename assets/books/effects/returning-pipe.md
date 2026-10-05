
# The Unwrap Pipe {.dqlh}

The **UNWRAP-PIPE** `!>` is a special pipe syntax
used often with certain built-in directives.

```delightql
employee(*), title = "IT Staff" !> assert!(count_is(2), "two IT staff")(*)
```

The above notation is sugar for

```delightql
employee(*), title = "IT Staff" |> assert!(count_is(2), "two IT staff")(*) |> .returned(*)
```

For certain primitive built-in directives
this notation permits a complete relation
to be returned from a directive efficiently.



