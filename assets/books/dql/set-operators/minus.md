
# Minus (Except) {.dqlh}

Minus returns rows from the first relation that have no match in the second.
A single operator `-`{.delightql .sigil} aligns by name:

```delightql
genre_2024(*) - genre_2025(*)
```

Rows in `genre_2024` with no corresponding row (by name) in `genre_2025`:
Rock And Roll and Easy Listening, although `genre_2025` stores its columns
in the other order.
Schemas must align -- if column names differ, rename first:

```delightql
employee(|> (employee_id)) - customer(|> (support_rep_id as employee_id))
```
