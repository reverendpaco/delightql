# In-line DDL {.dqlh}

A special notation allows DDL to be inlined in two
different contexts:

 - within a query context (most often at the REPL)
 - within a file to establish a sub-namespace

The former use-case allows for easy additions of
entities that become immediately auto-enlisted into the evaluation context:

```repl
∂> (~~ddl
outside(x) :- x < 10
outside(x) :- x > 15 ~~)  _(x @ -1; 5; 11;253), +outside(x)
  ->
┌─────┐
│ x   │
├─────┤
│ -1  │
│ 5   │
│ 253 │
└─────┘
∂> iota(20)(*), +outside(value)
  ->
┌───────┐
│ value │
├───────┤
│ 1     │
│ 2     │
│ 3     │
│ 4     │
│ 5     │
│ 6     │
│ 7     │
│ 8     │
│ 9     │
│ 16    │
│ 17    │
│ 18    │
│ 19    │
│ 20    │
└───────┘
```

The latter allows inline ddl within consulted files to create
a sub-namespace:

```{.delightql .am}
// example.dql

// plus_two is defined in a file
// and what namespace it will belong to
// is a function of how consult! is called
plus_two:(x) :- x + 2

(~~ddl:tmp
  // times_three is in a sub-namespace
  // whose complete namespace will be known
  // once the file is consulted
  times_three:(x) :- x * 3

~~)
```

If the above file is consulted into `lib::ops`,
then `times_three` as a function is namespaces as
`lib::ops::tmp.times_three`.
