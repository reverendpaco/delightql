
# Common Sigma Expressions {.dqlh}

Sigma predicates may be defined as common expressions
for use in the query.

```delightql
outside(x) : x < 0
outside(x) : x > 10
_(x @ -1; 5; 11), +outside(x) |> #(x)
```

Like rule-defined sigma predicates, the common sigma expressions
are detected by the compiler via classification of the clause body --
the expression defines a sigma expression when the body (inclusive of all clauses if applicable) is a scalar predicate.

```sql
SELECT
  anon_1.x AS x
FROM (
  SELECT -1 AS x
  UNION ALL
  SELECT 5
  UNION ALL
  SELECT 11
) AS anon_1
WHERE ((anon_1.x < 0 OR anon_1.x > 10)) IS TRUE;
```
