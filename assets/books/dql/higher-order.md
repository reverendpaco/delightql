# Higher-Order Pipes {.dqlh}

The **R-PIPE** `|>`{.delightql .sigil} passes a relation into a unary operator:
```delightql
employee(*), Salary > 5000
  |> ( LastName )
```

The fundamental unary operators -- projection, distinct, group by -- have
dedicated syntax covered in earlier sections:
```delightql
foo(*)  |>   ( LastName )
foo(*)  |>  -( FirstName, LastName )
foo(*)  |>  +( length:(LastName) as length_last_name )
foo(*)  |>  %( FirstName, LastName )
foo(*)  |>  %( FirstName, LastName ~> count:(*) )
```

Higher-order predicates extend this pattern. A programmer-defined higher-order
predicate can appear as the pipe target:
```delightql
employee(*)
  |> summarize(*)
```

Given this definition in assertion mode:
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

Higher-order parameters are relational terms passed BY REFERENCE: the
invocation preserves the written relation — filters, projections,
qualification and all — and the receiving definition (or built-in
rewrite) decides how and when to consume it. Constructing the
invocation implies no early evaluation or materialization.

Directive (bang) invocations share this exact shape and law: the first
parentheses hold the parameters, the trailing parentheses access the
return table — for a directive, its RECEIPT (`SEMANTICS/effect-algebra-law.md` §1,
§3). The first group is never an inline input table; a relational
input arrives through the pipe, by the same insertion law shown
above.

The piped form's advantage is composability with other pipe operators:
```delightql
batch.employee_2019(*), Salary > 50000,
  Department = "Engineering"
  |> clean_employees(*)
```

## Multi-Parameter Piped Invocation {.dqlh}

When the piped relation is not the first parameter, use `@` (the f-param
placeholder) to mark where it goes — borrowing function-pipe syntax:

```delightql
-- Definition: scalar first, table second
tagged(label, T(*))(*) :- T(*), ...

-- Piped with @:
users(*) |> tagged("young", @)(*)
```

Without `@`, the piped relation fills the first parameter by default. When
the first parameter is a scalar, this fails. The `@` placeholder makes the
target position explicit.
