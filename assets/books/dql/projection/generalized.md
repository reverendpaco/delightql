# Generalized Projection {.dqlh}

Columns can be transformed during projection using domain functions:

```delightql
employee(*)
    |>  ( upper:(FirstName) as f,
          upper:(LastName) as LastName,
          3 + Salary as salary_plus_three)
```

```sql
select
  upper(FirstName) as f,
  upper(LastName) as LastName,
  3 + Salary as salary_plus_three
from employee;
```

Note the colon in `upper:(FirstName).` This distinguishes functions from
relations -- `foo(A,B)` is a relation; `foo:(A)` is a function.

> Aggregate functions are not permitted in projection. See the sections on
> `distinct` and `group by` for aggregate usage. Delightql will reject known
> aggregates, but cannot detect user-defined aggregates--these will transpile as
> if scalar.

Other functions -- 'case', 'case select', concatenation, windowing/analytic
functions, and operators -- are covered in the function chapter of this reference.
