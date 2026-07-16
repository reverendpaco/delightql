# Context-Aware Functions {.dqlh}

A *context-aware* function closes over columns from its invocation context rather
than receiving them as explicit parameters.

```delightql
employee(*)
  |> ( cost_of_living:(..) as col_adjusted_salary)
```

The **UP-CONTEXT** sigil `..`{.delightql .sigil} signals that the function references columns from the
surrounding scope. It is required -- a reminder that the function is never truly
nullary.

Context-aware functions are defined with `..`{.delightql .sigil} in their signature:

```{.delightql .am}
// only works within the context of a
//  relation that has 'City' and 'Salary' columns

cost_of_living:( .. ) :-
  _:(  City in ("San Francisco";"Boston";"New York")
          -> Salary*0.8;
      City in ("Amarillo"; "Knoxville")
          -> Salary*1.45;
      _ -> Salary)
```

This function can be invoked on any relation with `City` and `Salary` columns -- the
free variables in its body. These columns are bound at the call site, not
passed explicitly.

Context-aware functions have no SQL counterpart; they are expanded inline
during transpilation.
