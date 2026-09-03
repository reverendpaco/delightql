# Tables Column Modality {.dqlh}

Any syntax of the form `foo(x,y)` means that the columns may be input (supplied) or
output (returned).

This matches the syntax and semantics of Prolog.

```delightql
foo(x,"bar")
```

In the above form, the `x` variable is output
and the `y` variable is input, producing a WHERE
predication via grounding:

```sql
select left_arg as x from foo where right_arg="bar";
```

It is important to understand that in Prolog-like argumentatitve join syntax
both columns are input at the same time :

```delightql
foo(x,"bar"),baz(x,2)
```

In the above the variable `x` is input (or bidirectionally unified) to both predicates,
creating a join.

The GLOB makes all columns into output columns:

```delightql
foo(*)
```

without having to enumerate all dimension positions.
