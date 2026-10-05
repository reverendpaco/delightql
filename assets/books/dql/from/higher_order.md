# Higher-Order Predicates {.dqlh}



Delightql supports higher-order predicates--predicates that accept tables (or
    scalars) as parameters and return a table. SQL calls these *table-valued
functions*. [Defining higher-order rules is covered in the DDL section.]{.sidenote}
Given this definition:

```{.delightql .am}
clean_contacts(T(*))( * ) :-
  T(*)
    |> $(trim:())( last_name, first_name)
    |> $(lower:())( email)
    |> -( fax)
```

a query passes it a table:

```delightql
clean_contacts(employee(*))(*)
```

Here, `clean_contacts` is a higher-order predicate that takes `employee(*)` as its
parameter.

The transpiled SQL depends on how the predicate was defined. With the
definition given, the query `clean_contacts(employee(*))(*)` produces:

```sql
select
    employee_id,
    trim(last_name)    as last_name,
    trim(first_name)   as first_name,
    title,
    reports_to,
    birth_date,
    hire_date,
    address,
    city,
    state,
    country,
    postal_code,
    phone,
    lower(email)      as email
from employee;
```

Higher-order predicates are structurally typed by the columns they reference.
[This resembles duck typing: delightql has no formal type layer, but detects
which columns the predicate body requires. Any table providing those columns is
a valid argument.]{.sidenote} In this example, any table with `last_name`,
`first_name`, `email`, and `fax` qualifies:

```delightql
clean_contacts(customer(*))(*)
```

The pipeline form (covered later) is equivalent:


```delightql
customer(*)
    |> clean_contacts(*)
```
