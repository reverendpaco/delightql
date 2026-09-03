
# The Unwrap Pipe {.dqlh}

The **UNWRAP-PIPE** `!>` is a special pipe syntax
used often with certain builtin directives.

```delightql
foo(*) !> bar!(*)
```

The above notation is sugar for

```delightql
foo(*) |> bar!(*) |> .returned(*)
```

For certain primitive builtin directives
this notation permits a complete relation
to be returned from a directive efficiently.



