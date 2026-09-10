
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



## Zero-Argument Functions and Citations {.dqlh}

A function with no parameters is defined with an empty head or, as shorthand,
with no parentheses at all. The two spellings define the same kind of value
function:
```delightql
greeting:() :- "hello"
tab :- char:(9)
```

Either is invoked as an ordinary application or, as shorthand, as a
**citation** — the name marked with a leading `:`{.delightql .sigil}:
```delightql
users(*) |> +(greeting:() as g, :tab as sep)
```

The body is any domain expression, not only a literal. The usual arity and
clause laws apply: `greeting:("x")` refuses, and defining `greeting` twice
without guards refuses whichever spelling each definition used.

A qualified citation puts the mark on the leaf, where the application form
puts its own mark: `lib::text.:greeting` invokes the same function as
`lib::text.greeting:()`. `:lib::text.greeting` is not a spelling.


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

