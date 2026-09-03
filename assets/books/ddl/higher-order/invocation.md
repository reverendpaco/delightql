# Direct Invocation {.dqlh}

Tables can be passed in as parameter arguments:

```delightql
department_employee_count(employee_2019(*), department_2019(*))(*)
```

The call site mirrors the definition head: each table parameter in this example is a
full functor expression.

Because call-site arguments are relation expressions, they can compose:

```delightql
department_employee_count(
  employee_2019(*, Salary > 50000),
  department_2019(*)
)(*)
```

Here the first argument is a filtered relation.

# Piped Invocation {.dqlh}

Pipes can be used on any higher-order predicate that takes
a table-valued parameter:

```delightql
clean_employees(T(*))(*) :-
  T(*) as t
    |> $(trim:())(t.LastName, t.FirstName)
    |> $(to_iso:())(t.BirthDate, t.HireDate)
```

```delightql
employee_2019(*)
  |> clean_employees(*)
```

The piped relation fills the last parameter.  The `(*)` after the
rule name is the output schema.

Chaining is possible:

```delightql
mask_ssn(mask_value,T(*))(*) :-
  T(*) |> $$(mask_value as ssn)
```

```delightql
employee_2019(*)
  |;| employee_2018(*)
  |;| employee_2017(*)
  |> clean_employees(*)
  |> mask_ssn("***-**-****")(*)
```

**Note**. As with function pipes, the relation is piped into the last parameter
of the higher-order predicate.  If the higher-order predicate has multiple
parameters, the other values must be set.

**Multi-parameter piped invocation.** When the piped relation is not the last
parameter, use `@` (the f-param placeholder) to mark where it goes -- the same
syntax as function pipes:

```delightql
-- Definition: scalar first, table second
tagged(T(*),label)(*) :- T(*), ...

-- Direct invocation (always works):
tagged(users(*),"young"))(*)

-- Piped invocation with @:
users(*) |> tagged(@,"young")(*)
```

The `@` tells the compiler which parameter receives the piped relation.
Without `@`, the piped relation fills the last parameter by default --
which fails when the last parameter is a scalar.
