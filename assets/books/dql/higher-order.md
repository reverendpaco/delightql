# Higher-Order Pipes {.dqlh}

The **R-PIPE** `|>`{.delightql .sigil} passes a relation into a unary operator:
```delightql
track(*), milliseconds > 600000
  |> ( name )
```

Delightql's built-in pipe unary operators -- projection, distinct, group by -- have
dedicated syntax:
```delightql
customer(*)  |>   ( last_name )
customer(*)  |>  -( first_name, last_name )
customer(*)  |>  +( length:(last_name) as length_last_name )
customer(*)  |>  %( first_name, last_name )
customer(*)  |>  %( country ~> count:(*) )
```

Higher-order predicats are programmer-defined rules
that can appear as the pipe target.
Given this example definition in assertion mode:
```{.delightql .am}
summarize(T(*))(*) :-
  T(*)
    |> %( ~>  count:(%composer)  as distinct_composer_count,
              count:(%genre_id)   as distinct_genre_count,
              count:(*)          as total_count,
              avg:(milliseconds) as average_milliseconds )
```

a pipe can target it:
```delightql
track(*)
  |> summarize(*)
```

The expression `track(*) |> summarize(*)`{.delightql} expands to:

```delightql
track(*)
    |> %( ~>  count:(%composer)  as distinct_composer_count,
              count:(%genre_id)   as distinct_genre_count,
              count:(*)          as total_count,
              avg:(milliseconds) as average_milliseconds )
```

## Piped vs. Direct Invocation {.dqlh}

Given this definition:
```{.delightql .am}
clean_employees(T(*))(*) :-
  T(*)
    |> $(trim:())(last_name, first_name)
    |> $(date:())(birth_date, hire_date)
```

Higher-order predicates can be invoked directly, passing full functor
expressions:
```delightql
clean_employees(main.employee(*))(*)
```

This is equivalent to:
```delightql
main.employee(*)
  |> clean_employees(*)
```

Direct invocation accepts any relation expression, including filters
and projections:
```delightql
clean_employees(main.employee(*, hire_date > "2003-01-01"))(*)
```


Higher-order parameters are passed by reference: their
invocation preserves the written relation without evaluation.


The piped form's advantage is composability with other pipe operators:
```delightql
main.employee(*), hire_date > "2003-01-01",
  title = "Sales Support Agent"
  |> clean_employees(*)
```

## Multi-Parameter Piped Invocation {.dqlh}

When the piped relation is not the last parameter, use `@` (the f-param
placeholder) to mark where it goes -- borrowing function-pipe syntax:

```delightql
// Definition: scalar second, table first
tagged(T(*),label)(*) : T(*) |> +($.label as tag)

// Piped with @:
customer(*) |> tagged(@,"vip")(*)
```

