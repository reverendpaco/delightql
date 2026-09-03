
# Insert {.dqlh}

Use the `insert!` pseudo-predicate to insert rows.
The relation entering the `insert!` pipe must contain
a subset of the schema of the mutation target.  Any extra or erroneously
named columns are an error.

```delightql
_(LastName, FirstName, age @ "eklund", "daniel", 20)
  |> insert!(employee(*))(*)
```

```sql
INSERT INTO hr.employee (LastName, FirstName, age)
VALUES ('eklund', 'daniel', 20);
```

You may union tables and prediacte their tuples to provide input tuples:

```delightql
hr.employee(*)
  |;| new_hires(*)
  |;| transfers(, effective_date = today:())
  |> insert!(employee(*))(*)
```


```delightql
candidates(*),
  score > 90 |> (name, Department, start_date)
  |> insert!(hr.employee(*))(*)
```

```sql
INSERT INTO hr.employee (name, Department, start_date)
SELECT name, Department, start_date
FROM candidates
WHERE score > 90;
```
