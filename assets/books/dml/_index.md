# Data Manipulation Language (DML) {.dqlh}

Delightql supports SQL's tree mutation operations through three destructively sigilized pipe targets:

- `update!(T(*))(*)` --  modify existing rows
- `insert!(T(*))(*)` --  add new rows
- `delete!(T(*))(*)` --  remove rows

The `T(*)` is the **mutation target** -- a functor expression identifying
which relation to mutate.

## The `!!` Marker {.dqlh}

For `update!` and `delete!`, the source relation is also the
mutation target -- the rows being sourced are the rows being mutated.
Mark the source with `!!` to make this explicit:

```delightql
hr.employee!!(*)                   // !! = "these rows will be mutated"
  , Department = "Executive"
  |> delete!(hr.employee(*))(*)
```

The `!!` marker is required when the source is the mutation target.  The
compiler verifies that the `!!`-marked relation matches the terminal target.

For `insert!`, the source rows are **read-only input** -- even when the
source table happens to be the same as the target.  Do not use `!!` on
insert sources:

```delightql
employees(*)                       // no !! -- these rows are read-only
  , department = "Engineering"
  |> (id + 10 as id, name, department, age, salary)
  |> insert!(employees(*))(*)
```

| Terminal | Source has `!!`? | Reason |
|----------|-----------------|--------|
| `update!` | Yes | Source rows are modified in place |
| `delete!` | Yes | Source rows are removed |
| `insert!` | No | Source rows are read-only input |



