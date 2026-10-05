# Generalized Projection {.dqlh}

Columns can be transformed during projection using domain functions:

```delightql
track(*)
    |>  ( upper:(name) as n,
          upper:(composer) as composer,
          milliseconds / 1000 as seconds)
```

```sql
select
  upper(name) as n,
  upper(composer) as composer,
  milliseconds / 1000 as seconds
from track;
```

Note the colon in `upper:(name).` This distinguishes functions from
relations -- `foo(A,B)` is a relation; `foo:(A)` is a function.

> Aggregate functions are not permitted in projection. See the sections on
> `distinct` and `group by` for aggregate usage. Delightql will reject known
> aggregates, but cannot detect user-defined aggregates--these will transpile as
> if scalar.

Other functions -- 'case', 'case select', concatenation, windowing/analytic
functions, and operators -- are covered in the function chapter of this reference.
