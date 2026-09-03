
# Minus (Except) {.dqlh}

Minus returns rows from the first relation that have no match in the second.
A single operator `-`{.delightql .sigil} aligns by name:

```delightql
employee_current(*) - employee_terminated(*)
```

Rows in `employee_current` with no corresponding row (by name) in `employee_terminated`.
Schemas must align -- if column names differ, rename first:

```delightql
employee_current(*) - employee_terminated(|> *(emp_id as id))
```
