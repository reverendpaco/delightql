
# Glob Parameter Functors {.dqlh}

An **glob parameter functor** `T(*)` is **structurally/duck typed**: the body
references columns by name, and any table that has those columns is
accepted regardless of extra columns.

```delightql
clean_employees(T(*))(*) :-
  T(*) as t
    |> $(trim:())(t.LastName, t.FirstName)
    |> $(to_iso:())(t.BirthDate, t.HireDate)
```

The parameter `T(*)` accepts any table with `LastName`, `FirstName`,
`BirthDate`, and `HireDate` columns.

