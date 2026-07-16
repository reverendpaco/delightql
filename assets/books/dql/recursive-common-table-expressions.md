
# Recursive Common Table Expressions {.dqlh}

A common table expression becomes recursive when one of its defining clauses references the CTE being defined. Delightql detects this self-reference and emits WITH RECURSIVE.

**Sequence generation**:

```delightql
_(n @ 1) : nums
nums(*), n < 100 |> (n + 1 as n) : nums
nums(*) ~> sum:(n)
```
```sql
WITH RECURSIVE nums(n) AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM nums WHERE n < 100
)
SELECT sum(n) FROM nums;
```

The first clause seeds the CTE with 1. The second clause references nums and
increments until the condition fails. This is the standard pattern: base case,
then recursive case with termination condition.

**Multiple clauses accumulate**:

Non-recursive CTEs can have multiple clauses that combine via UNION ALL:

```delightql
users_2022(*) |> (first_name, last_name) : names
users_2023(*) |> (first_name, last_name) : names
users_2024(*) |> (first_name, last_name) : names
names(*)
```

When any clause references the CTE name, the entire CTE becomes recursive:

```delightql
_(x @ 1) : nums
_(x @ 100) : nums
nums(*), x < 200 |> (x + 1 as x) : nums
nums(*)
```

Here, two anchor clauses (starting at 1 and 100) seed the recursion. Both
sequences grow until reaching 200.
