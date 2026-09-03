# Higher-Order Pipes {.dqlh}

The **R-PIPE** `|>`{.delightql .sigil} passes a relation into a unary operator:
```delightql
employee(*), Salary > 5000
  |> ( LastName )
```

Delightql's builtin pipe unary operators -- projection, distinct, group by -- have
dedicated syntax:
```delightql
foo(*)  |>   ( LastName )
foo(*)  |>  -( FirstName, LastName )
foo(*)  |>  +( length:(LastName) as length_last_name )
foo(*)  |>  %( FirstName, LastName )
foo(*)  |>  %( FirstName, LastName ~> count:(*) )
```

Higher-order predicats are programmer-defined rules
that can appear as the pipe target:
```delightql
employee(*)
  |> summarize(*)
```

Given this example definition in assertion mode:
```{.delightql .am}
summarize(T(*))(*) :-
  T(*)
    ~> ( count:(%LastName)  as distinct_last_name_count,
         count:(%Department) as distinct_department_count,
         count:(*)           as total_count,
         avg:(Salary)        as average_salary )
```

the expression `employee(*) |> summarize(*)`{.delightql} expands to:

```delightql
employee(*)
  ~> ( count:(%LastName)  as distinct_last_name_count,
       count:(%Department) as distinct_department_count,
       count:(*)           as total_count,
       avg:(Salary)        as average_salary )
```

## Piped vs. Direct Invocation {.dqlh}

Higher-order predicates can be invoked directly, passing full functor
expressions:
```delightql
clean_employees(batch.employee_2019(*))(*)
```

This is equivalent to:
```delightql
batch.employee_2019(*)
  |> clean_employees(*)
```

Direct invocation accepts any relation expression, including filters
and projections:
```delightql
clean_employees(batch.employee_2019(*, Salary > 50000))(*)
```


Higher-order parameters are passed by reference: their
invocation preserves the written relation without evaluation.


The piped form's advantage is composability with other pipe operators:
```delightql
batch.employee_2019(*), Salary > 50000,
  Department = "Engineering"
  |> clean_employees(*)
```

## Multi-Parameter Piped Invocation {.dqlh}

When the piped relation is not the last parameter, use `@` (the f-param
placeholder) to mark where it goes -- borrowing function-pipe syntax:

```delightql
-- Definition: scalar second, table first
tagged(T(*),label)(*) :- T(*), ...

-- Piped with @:
users(*) |> tagged(@,"young")(*)
```

