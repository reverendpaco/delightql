
# Glob Parameter Functors {.dqlh}

An **glob parameter functor** `T(*)` is **structurally/duck typed**: the body
references columns by name, and any table that has those columns is
accepted regardless of extra columns.

```{.delightql .am}
clean_employees(T(*))(*) :-
  T(*)
    |> $(trim:())(last_name, first_name)
    |> $(date:())(birth_date, hire_date)
```

The parameter `T(*)` accepts any table with `last_name`, `first_name`,
`birth_date`, and `hire_date` columns.

