
# Argumentative Parameter Functors {.dqlh}

An **argumentative paramter functor** `T(a, b)` is **positionally typed**:
the input must have exactly two columns, and they are renamed to `a` and
`b` inside the body.  The caller's original column names are overwritten.

```delightql
foo(T(label, value))(*) :-
  T(*), value > 10 |> (label)
```

The names `label` and `value` are column aliases available in the body.
The definition simultaneously declares the arity (two columns) and
provides names for positional access.

The advantage of the argumentative functor is in the calling convention,
called *scalar lifting*.  Because the definition declares a positional
contract, a call site *may* pass bare scalars instead of a table:

```delightql
foo("first", 2)(*)
```

The scalars are positionally matched to the declared columns `label` and
`value` and lifted into a one-row table.  This cascades to stacked notation:

```delightql
foo("first", 2; "second", 20)(*)
```

which sugars explicit anonymous tables:

```delightql
foo(_("first", 2; "second", 20))(*)
```

but still allows pipe invocation:

```delightql
two_column_table(*)
  |> foo(*)
```

Or explicit functor invocation:

```delightql
foo(two_column_table(*))(*)
```

Scalar lifting requires a positional contract -- a glob parameter functor
cannot accept inline scalars because there is no declared arity to match
against.


## The `&` Rule {.dqlh}

**`&` is required only when using scalar lifting with an argumentative
functor alongside other parameters.**

When every table argument is passed as a functor expression, the
parentheses disambiguate each argument. Commas separate parameters
as usual -- no `&` needed.  `&` is the cost of the scalar-lifting
shorthand: when bare scalars fill an argumentative functor, `&`
marks where one argument ends and the next begins.

