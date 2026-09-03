
# Domain {.dqlh}

The term domain is used throughout this book much in the same way it is used throughout
mathematics, as the set of possible values for a given quantity.  In SQL parlance, this is not to be confused
with type, as the domain of heights should not be confused with the domain of Kelvin temperatures
even though both could be modeled as positive real numbers.

Highlighting the term domain is done to create an emphasis on non-domain things.  Delightql
makes this distinction by asserting that things are either domain values or predicate values,
either domain expressions or predicate expressions.

This is a somewhat artificial delineation as tables/predicates being cartesian products have
their own very real input space and domain.


To be concrete:

  - **Domain values** include `1`, `"roger"`, and `true`; domain
expressions include `1+2` and `upper("roger")`.
  - **Predicate values** include
`employee`; predicate expressions include `generate_series(1,10)` or `(select
i_am_a_subquery from inner) as newtable`.

