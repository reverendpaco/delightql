
# Higher-Order Functions {.dqlh}

Functions are inherently higher-order: any function can accept other functions
as parameters. Mark function parameters with colon-functor syntax `f:()` in the
signature to distinguish them from scalar parameters:

```delightql
apply:(f:(), x) :- f:(x)
```

The `f:()` declares that the first parameter is a function. The body calls
whatever function was passed in. Scalar parameters are bare names as usual.

**Invocation:**
```delightql
users(*) |> (apply:(upper:(), first_name) as formatted)
```

The call site passes `upper:()` (a curried function) and `first_name` (a column)
as two arguments. Arity matching works the same as regular functions: `apply` has
arity 2, and the call provides 2 arguments.

**Multiple function parameters:**
```delightql
chain:(f:(), g:(), x) :- x /-> f:() /-> g:()
```
```delightql
users(*) |> (chain:(upper:(), trim:(), first_name) as cleaned)
```

**Lambda as function argument:**
```delightql
apply_twice:(f:(), x) :- x /-> f:() /-> f:()
```
```delightql
users(*) |> (apply_twice:(:(@ * 2), age) as quadrupled)
```

**Mixed function and scalar parameters:**
```delightql
transform_and_compute:(f:(), g:(), value, multiplier) :-
  f:(value) /-> g:() /-> :(@ * multiplier)
```

**With conditional logic:**
```delightql
apply_if_long:(f:(), value) :-
  _:(length:(value) > 5 -> f:(value); _ -> value)
```

**No double parentheses.** Unlike higher-order views, higher-order functions use
a single set of parentheses. Views need double parens because they operate on two
modal categories -- input-only parameters (tables) and bidirectional columns. Functions
have no such distinction: everything is a value in, scalar out. See
[Higher-Order Rules](../advanced/higher-order.md) for the full rationale.

