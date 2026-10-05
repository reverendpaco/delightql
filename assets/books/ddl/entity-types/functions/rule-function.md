
# Rule Form {.dqlh}

For computed functions, use the rule form:
```{.delightql .am}
plus_two:(x) :- x + 2
```
```delightql
invoice(*) |> +(plus_two:(total) as incremented)
```
```sql
SELECT *, total + 2 AS incremented FROM invoice;
```

The body is any domain expression. The function returns its evaluation.



## Zero-Argument Functions and Citations {.dqlh}

A function with no parameters is defined with an empty head or, as shorthand,
with no parentheses at all. The two spellings define the same kind of value
function:
```{.delightql .am}
greeting:() :- "hello"
tab :- char:(9)
```

Either is invoked as an ordinary application or, as shorthand, as a
**citation** — the name marked with a leading `:`{.delightql .sigil}:
```delightql
artist(*) |> +(greeting:() as g, :tab as sep)
```

The body is any domain expression, not only a literal. The usual arity and
clause laws apply: `greeting:("x")` refuses, and defining `greeting` twice
without guards refuses whichever spelling each definition used.

A qualified citation puts the mark on the leaf, where the application form
puts its own mark: `lib::text.:greeting` invokes the same function as
`lib::text.greeting:()`. `:lib::text.greeting` is not a spelling.


## Disjunctive Clauses {.dqlh}

Multiple clauses create conditional functions. Clauses are evaluated top-to-bottom; first match wins:
```{.delightql .am}
fizzbuzz:(n | (n % 15) = 0) :- "fizzbuzz"
fizzbuzz:(n | (n % 3) = 0)  :- "fizz"
fizzbuzz:(n | (n % 5) = 0)  :- "buzz"
fizzbuzz:(n)              :- n
```

The guard condition follows `|` in the head. If the guard fails, the next clause is tried.
```delightql
_(n @ 1; 2; 3; 4; 5; 6; 7; 8; 9; 10; 11; 12; 13; 14; 15) |> (fizzbuzz:(n) as result)
```
```sql
SELECT
  CASE
    WHEN n % 15 IS NOT DISTINCT FROM 0 THEN 'fizzbuzz'
    WHEN n % 3 IS NOT DISTINCT FROM 0 THEN 'fizz'
    WHEN n % 5 IS NOT DISTINCT FROM 0 THEN 'buzz'
    ELSE n
  END AS result
FROM (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3
      UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6
      UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
      UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
      UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15);
```

**Hailstone sequence example:**
```{.delightql .am}
next_hailstone:(x | (x % 2) = 0) :- x / 2
next_hailstone:(x)             :- (x * 3) + 1
```


The `@` marks where the piped value is inserted when the function takes multiple arguments.

