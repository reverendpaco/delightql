
# Delete {.dqlh}

To delete from a table,  use predication to select
the rows that should be removed. The mutation target
must also be the source table and the schemas must match.


```delightql
hr.employee!!(*)
  , Department = "Executive"
  |> delete!(hr.employee(*))(*)
```

```sql
DELETE FROM hr.employee
WHERE Department = 'Executive';
```

Without filters, all rows are deleted:

```delightql
hr.employee!!(*) |> delete!(hr.employee(*))(*)
```

```sql
DELETE FROM hr.employee;
```

To keep only some rows, invert the predicate and delete the
complement:

```delightql
hr.employee!!(*)
  , Department != "Engineering"
  |> delete!(hr.employee(*))(*)
```

