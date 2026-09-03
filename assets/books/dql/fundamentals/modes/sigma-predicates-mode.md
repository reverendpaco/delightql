
# Sigma Predicates Column Modality {.dqlh}

All calls of the form `+foo(x,y)` or `\+ foo(x,y)` are EXISTS/NOT-EXISTS sigma-predicate invocations.


```delightql
orders(*),
  +empty(comments),
  +like(description,"%widget")
```

Delightql requires that all columns be instantiated as input.

Sigma predicates defined as re-usable rules,
must only use forms in their body that pass the input
moding to their implementation:


```{.delightql .numberLines .am}
no_data("NA"; "N/A"; "UNKNOWN")

empty(column) :- null = column
empty(column) :- trim:(column) = ""
empty(column) :- +no_data(upper:(column))
```

Note that all built-in predicates, like `=` and `<`
and built-in functions, like `+` and `*` are also
input-only moded.


Observing the definition of a rule consist of calls to
input-moded predicates is enough to distinguish this rule from
table rules.

The invocation of this rule will **always** be discernible, by virtue
of requiring a `+` or `\+` EXISTS marker before the predicate name:

```delightql
orders(*),
  +empty(comments)
```


