
# Delete {.dqlh}

To delete from a table,  use predication to select
the rows that should be removed. The mutation target
must also be the source table and the schemas must match.


```delightql
employee!!(*)
  , title = "IT Staff"
  |> delete!(employee(*))(*)
```

```sql
DELETE FROM employee
WHERE title = 'IT Staff';
```

Without filters, all rows are deleted:

```delightql
invoice_line!!(*) |> delete!(invoice_line(*))(*)
```

```sql
DELETE FROM invoice_line;
```

To keep only some rows, invert the predicate and delete the
complement:

```delightql
invoice_line!!(*)
  , unit_price != 0.99
  |> delete!(invoice_line(*))(*)
```

