
# Rule Form {.dqlh}

For computed functions, use the rule form:
```delightql
plus_two:(x) :- x + 2
```
```delightql
numbers(*) |> +(plus_two:(value) as incremented)
```
```sql
SELECT *, value + 2 AS incremented FROM numbers;
```

The body is any domain expression. The function returns its evaluation.



## Disjunctive Clauses {.dqlh}

Multiple clauses create conditional functions. Clauses are evaluated top-to-bottom; first match wins:
```delightql
fizzbuzz:(n | (n % 15) = 0) :- "fizzbuzz"
fizzbuzz:(n | (n % 3) = 0)  :- "fizz"
fizzbuzz:(n | (n % 5) = 0)  :- "buzz"
fizzbuzz:(n)              :- n
```

The guard condition follows `|` in the head. If the guard fails, the next clause is tried.
```delightql
generate_series(1, 100)(*) |> (fizzbuzz:(value) as result)
```
```sql
SELECT
  CASE
    WHEN value % 15 = 0 THEN 'fizzbuzz'
    WHEN value % 3 = 0 THEN 'fizz'
    WHEN value % 5 = 0 THEN 'buzz'
    ELSE CAST(value AS TEXT)
  END AS result
FROM generate_series(1, 100);
```

**Hailstone sequence example:**
```delightql
next_hailstone:(x | (x % 2) = 0) :- x / 2
next_hailstone:(x)             :- (x * 3) + 1
```


The `@` marks where the piped value is inserted when the function takes multiple arguments.

