# Function Rules {.dqlh}

Functions are relations with a **functional dependency** between input and
output columns. Where a relation may have many outputs for a given input, a
function has exactly one. Delightql supports several syntactic forms for
defining functions, each suited to different use cases.

## Stacked Notation (Named Case) {.dqlh}

The stacked form defines functions as lookup tables with explicit input-output mappings:
```delightql
department_kind(
  department     -> kind
  ------------------
  "engineering"  -> "tech";
  "data science" -> "tech";
  _              -> "other"
)
```

The `->` separates inputs (left) from outputs (right). The header row names the columns; subsequent rows provide the mappings. The `_` matches any input not explicitly listed.

Despite the visual similarity to anonymous table stacked notation, this is an assertion-mode construct -- it defines a reusable function, not inline data.

**Invocation:**
```delightql
employee(*) |> +(department_kind:(Department) as kind)
```
```sql
SELECT *,
  CASE Department
    WHEN 'engineering' THEN 'tech'
    WHEN 'data science' THEN 'tech'
    ELSE 'other'
  END AS kind
FROM employee;
```

**Multi-column inputs:**
```delightql
tax_rate(
  state, category -> rate
  --------------------------
  "CA", "food"    -> 0.0;
  "CA", "electronics" -> 0.0825;
  "TX", "food"    -> 0.0;
  "TX", "electronics" -> 0.0625;
  _, _            -> 0.05
)
```
```delightql
products(*) |> +(tax_rate:(state, category) as tax)
```


## Rule Form {.dqlh}

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
fizzbuzz:(n | n % 15 = 0) :- "fizzbuzz"
fizzbuzz:(n | n % 3 = 0)  :- "fizz"
fizzbuzz:(n | n % 5 = 0)  :- "buzz"
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
next_hailstone:(x | x % 2 = 0) :- x / 2
next_hailstone:(x)             :- (x * 3) + 1
```

## Composition Notation {.dqlh}

Point-free function composition uses the F-PIPE sigil:
```delightql
clean:(@) :- trim:() /-> upper:()
```

Equivalent to:
```delightql
clean:(x) :- upper:(trim:(x))
```

The piped form reads left-to-right, matching data flow.

**With placeholder:**
```delightql
birth_year:(@) :- strftime:("%Y", @) /-> cast:(@ as int)
```

The `@` marks where the piped value is inserted when the function takes multiple arguments.

## Higher-Order Functions {.dqlh}

Functions are inherently higher-order: any function can accept other functions
as parameters. Mark function parameters with colon-functor syntax `f:()` in the
signature to distinguish them from scalar parameters:

```delightql
apply:(f:(), x) :- f:(x)
```

The `f:()` declares that the first parameter is a function. The body calls
whatever function was passed in. Scalar parameters are bare names as usual.

**Invocation:**
```delightql
users(*) |> (apply:(upper:(), first_name) as formatted)
```

The call site passes `upper:()` (a curried function) and `first_name` (a column)
as two arguments. Arity matching works the same as regular functions: `apply` has
arity 2, and the call provides 2 arguments.

**Multiple function parameters:**
```delightql
chain:(f:(), g:(), x) :- x /-> f:() /-> g:()
```
```delightql
users(*) |> (chain:(upper:(), trim:(), first_name) as cleaned)
```

**Lambda as function argument:**
```delightql
apply_twice:(f:(), x) :- x /-> f:() /-> f:()
```
```delightql
users(*) |> (apply_twice:(:(@ * 2), age) as quadrupled)
```

**Mixed function and scalar parameters:**
```delightql
transform_and_compute:(f:(), g:(), value, multiplier) :-
  f:(value) /-> g:() /-> :(@ * multiplier)
```

**With conditional logic:**
```delightql
apply_if_long:(f:(), value) :-
  _:(length:(value) > 5 -> f:(value); _ -> value)
```

**No double parentheses.** Unlike higher-order views, higher-order functions use
a single set of parentheses. Views need double parens because they operate on two
modal categories -- input-only parameters (tables) and bidirectional columns. Functions
have no such distinction: everything is a value in, scalar out. See
[Higher-Order Rules](../advanced/higher-order.md) for the full rationale.

## Contextual Functions {.dqlh}

The `..` sigil indicates a function that captures variables from its invocation context:
```delightql
excess_index:(..) :-
  (1 + total - (interest_rate / 252))
    /-> greatest:(0.01)
    /-> ln:()
    /-> :(@ * 2)
    /-> sum:(<~ #(date))
    /-> exp:()
```
```delightql
prices(*) |> (excess_index:(..) as idx)
```

The function analyzes its body for free variables (`total`, `interest_rate`,
`date`) and expects them from the calling relation. This is structural typing
for functions -- any relation with those columns can use the function.

**Mixed parameters:**

Combine context capture with explicit arguments:
```delightql
scaled_index:(.., scale_factor) :-
  (1 + total - (interest_rate / 252))
    /-> greatest:(0.01)
    /-> ln:()
    /-> :(@ * scale_factor)
    /-> exp:()
```
```delightql
prices(*) |> (
  scaled_index:(.., 2) as double_scaled,
  scaled_index:(.., 0.5) as half_scaled
)
```

**Named context:**

Explicitly declare captured variables:
```delightql
scaled_index:(..{total, interest_rate}, scale_factor) :-
  (1 + total - (interest_rate / 252))
    /-> greatest:(0.01)
    /-> :(@ * scale_factor)
    /-> exp:()
```

This makes dependencies visible in the signature and allows overriding context with explicit values:
```delightql
prices(*) |> (
  scaled_index:(.., 2) as from_context,
  scaled_index:(manual_total, manual_rate, 2) as explicit
)
```

## Fact Form {.dqlh}

Individual facts define point mappings:
```delightql
department_kind:("engineering" -> "tech")
department_kind:("data science" -> "tech")
department_kind:(_ -> "other")
```

This is equivalent to the stacked form but spread across statements. Use it when mappings are added incrementally or loaded from external sources.

## Restrictions {.dqlh}

- Stacked notation and rule form cannot be mixed for the same function
- Stacked notation and individual facts cannot be mixed for the same function
- Disjunctive clauses (multiple rules with the same head) must be co-located in the source
- Textual order determines evaluation order for disjunctive clauses
