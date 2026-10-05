
# Argumentative Access {.dqlh}

Arguments within the functor bring columns into scope by position.

```{.delightql .widen}
employee(employee_id  , last_name ,
         first_name   , title    , reports_to,
         birth_date   , hire_date , address, city,
         state       , country  , postal_code,
         phone       , fax      , email )
```

This binds each identifier to the column at that ordinal position in the table
definition. The arity must match exactly -- if `employee` has 15 columns, the
functor must have 15 arguments.

The identifiers you choose become the column names in scope of the position having
the identifier.

```delightql
employee(a, b,  c,  d,
        e,   f, g,   h, i,
        j,  k,   l, m,   n,  o )
```

This makes argumentative access error-prone for wide tables. Prefer named access
(covered later) when arity exceeds a handful of columns.

