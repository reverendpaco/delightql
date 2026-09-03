
# The `in` Predicate {.dqlh}

```delightql
employee(*), +_(State@"MA";"TX";"AK";"AR")
```

Syntactic sugar provides the familiar form:

```delightql
employee(*), State in ("MA";"TX";"AK";"AR")
```

Both transpile to:

```sql
select
  *
from employee where State in ('MA','TX','AK','AR');
```


The unsugared form generalizes to multi-column comparisons:

```delightql
employee(*), +_( State, Department @
                 "MA","Engineering";
                 "TX","Engineering";
                 "CA","Sales")
```

```sql
SELECT *
FROM employee
WHERE
  ('MA' IS NOT DISTINCT FROM State
  AND 'Engineering' IS NOT DISTINCT FROM Department)
  OR ('TX' IS NOT DISTINCT FROM State
  AND 'Engineering' IS NOT DISTINCT FROM Department)
  OR ('CA' IS NOT DISTINCT FROM State
  AND 'Sales' IS NOT DISTINCT FROM Department);
```
