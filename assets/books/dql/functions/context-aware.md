# Context-Aware Functions {.dqlh}

A *context-aware* function closes over columns from its invocation context rather
than receiving them as explicit parameters.

Context-aware functions are defined with `..`{.delightql .sigil} in their signature:

```{.delightql .am}
// only works within the context of a
//  relation that has 'billing_city' and 'Total' columns

cost_of_living:( .. ) :-
  _:(  billing_city in ("Mountain View";"Boston";"New York")
          -> total*0.8;
      billing_city in ("Fort Worth"; "Tucson")
          -> total*1.45;
      _ -> total)
```

and called with it:

```delightql
invoice(*)
  |> ( cost_of_living:(..) as col_adjusted_total)
```

The **UP-CONTEXT** sigil `..`{.delightql .sigil} signals that the function references columns from the
surrounding scope. It is required -- a reminder that the function is never truly
nullary.

This function can be invoked on any relation with `billing_city` and `total` columns -- the
free variables in its body. These columns are bound at the call site, not
passed explicitly.

Context-aware functions have no SQL counterpart; they are expanded inline
during transpilation.
