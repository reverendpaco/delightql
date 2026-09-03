
# Positional (Traditional) Union All (`||`{.delightql .sigil}) {.dqlh}

**Aligns by position**. Requires identical column count. Useful for intentional realignment.[The below
example uses interior relations to shape each relation prior to the `UNION
ALL`.]{.sidenote}:

```delightql
users_2024(|> (last_name,first_name,age))
  ||
users_2023(|> (LastName,First,Age))
```
