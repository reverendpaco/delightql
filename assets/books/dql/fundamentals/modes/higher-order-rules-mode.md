# Higher-Order Predicates Column Modality {.dqlh}

The other name for **higher-order predicates** is **input-moded predicates**.

They are discernible in code by having multiple parentheses, or
by being the target of a relational pipe:

```delightql
customer(*)
  |> at_least(50)(*)
```

```delightql
at_least(50, customer(*))(*)
```

The first parentheses (the ones to the left and closest to the name of the higher-order rule) contain only input-only columns. The columns in the second parentheses are treated like table columns which are input-or-output.

Again, a literal reference is not required to trigger input instantiation:

```delightql
_(val @ 1;2;3;4), iota(val)(*)
```

In the above example, the anonymous table's lone column `val` becomes the input to the `iota` parameter set.
