# Delegate Selection {.dqlh}

A grouped query collapses each group to one row. Aggregate functions
*synthesize* a value across the group (`sum`, `count`, `avg`). **Delegate
selection** does the opposite: it *selects* a value that already exists in
some row of the group, using the F-OVER sigil `<~`{.delightql .sigil} in
reduction place to pull the reduction back to a representative row.

A delegate payload is parenthesized for a coherent multi-column row, or a
bare column for a single value. With an empty ordering (bare `<~`) the
delegate row is **arbitrary**:

```delightql
  employee(*)
    |> %(Department ~> (LastName, FirstName) <~)
```

`LastName` and `FirstName` are read from the *same* arbitrary row of each
Department group. This replaces the older `~?` "unsafe reduced column"
sigil. [SQLite provides useful semantics when an aggregate such as `max()`
or `min()` is also present -- the delegate leans toward the row containing
the extremum.]{.sidenote}

```delightql
  employee(*)
    |> %(Department
          ~>
        max:(Salary),
        (LastName, FirstName) <~)
```

[An *ordered* delegate `<~ #(order)` selects the first row under an
explicit ordering -- the general form that subsumes Postgres `DISTINCT
ON`. That form is forthcoming.]{.sidenote}
