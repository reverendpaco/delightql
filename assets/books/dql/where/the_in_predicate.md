
# The `in` Predicate {.dqlh}

```delightql
customer(*), +_(state@"MA";"TX";"CA";"ON")
```

Syntactic sugar provides the familiar form:

```delightql
customer(*), state in ("MA";"TX";"CA";"ON")
```

Both ask whether the customer's state corresponds to one candidate.
With these non-NULL constants, the filtering SQL can be written:

```sql
select
  *
from customer where state in ('MA','TX','CA','ON');
```


The unsugared form generalizes to multi-column comparisons:

```delightql
customer(*), +_( country, state @
                 "USA","CA";
                 "USA","TX";
                 "Canada","ON")
```

```sql
SELECT *
FROM customer
WHERE
  ('USA' = country
  AND 'CA' = state)
  OR ('USA' = country
  AND 'TX' = state)
  OR ('Canada' = country
  AND 'ON' = state);
```

`+_` is an existential observer of the same anonymous table an unmarked
comma member would join. When a column from one row is matched to a
candidate row, NULL never establishes correspondence, even under `+`.
For example, `_(a @ null), +_(a @ null)` has no answer. A wholly ground
`null in (null)` is a local value test instead and remains true.
