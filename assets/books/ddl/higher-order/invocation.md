# Direct Invocation {.dqlh}

Tables can be passed in as parameter arguments. Given this definition:

```{.delightql .am}
genre_track_count(T(*), G(*))(genre, track_count) :-
  T(*), G(*.(genre_id))
    |> %(G.name as genre ~> count:(*) as track_count)
```

a call passes two tables:

```delightql
genre_track_count(track(*), genre(*))(*)
```

The call site mirrors the definition head: each table parameter in this example is a
full functor expression.

Because call-site arguments are relation expressions, they can compose:

```delightql
genre_track_count(
  track(*, milliseconds > 300000),
  genre(*)
)(*)
```

Here the first argument is a filtered relation.

# Piped Invocation {.dqlh}

Pipes can be used on any higher-order predicate that takes
a table-valued parameter:

```{.delightql .am}
clean_employees(T(*))(*) :-
  T(*)
    |> $(trim:())(last_name, first_name)
    |> $(date:())(birth_date, hire_date)
```

```delightql
employee(*)
  |> clean_employees(*)
```

The piped relation fills the last parameter.  The `(*)` after the
rule name is the output schema.

Chaining is possible:

```{.delightql .am}
mask_phone(mask_value,T(*))(*) :-
  T(*) |> $$($.mask_value as phone)
```

```delightql
employee_2024(*)
  |;| employee_2025(|> *(job_title as title, manager_id as reports_to))
  |> clean_employees(*)
  |> mask_phone("+1 (***) ***-****")(*)
```

**Note**. As with function pipes, the relation is piped into the last parameter
of the higher-order predicate.  If the higher-order predicate has multiple
parameters, the other values must be set.

**Multi-parameter piped invocation.** When the piped relation is not the last
parameter, use `@` (the f-param placeholder) to mark where it goes -- the same
syntax as function pipes:

```delightql
// Definition: table first, scalar second
tagged(T(*),label)(*) : T(*) |> +($.label as tag)

// Direct invocation (always works):
tagged(customer(*),"vip")(*)

// A `:` definition lasts one query, so it is restated:
tagged(T(*),label)(*) : T(*) |> +($.label as tag)

// Piped invocation with @:
customer(*) |> tagged(@,"vip")(*)
```

The `@` tells the compiler which parameter receives the piped relation.
Without `@`, the piped relation fills the last parameter by default --
which fails when the last parameter is a scalar.
