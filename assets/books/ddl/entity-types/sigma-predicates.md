# Sigma Rules {.dqlh}

Sigma rules encapsulate reusable boolean logic:

```delightql
is_high_value(amount) :- amount > 1000
```

```delightql
orders(*),
  +is_high_value(total),
  +like(description, '%ipod')
```


**Disjunctive Clauses**

Multiple clauses with the same head are OR-ed together:

```{.delightql .numberLines .am}
no_data("NA"; "N/A"; "UNKNOWN")

empty(column) :- null = column
empty(column) :- trim:(column) = ""
empty(column) :- +no_data(upper:(column))
```

```delightql
employee(*),
  +empty(LastName),
  +empty(FirstName)
```

```sql
SELECT *
FROM employee
WHERE (LastName IS NULL
       OR trim(LastName) = ''
       OR upper(LastName)
        IN ('NA', 'N/A', 'UNKNOWN'))
  AND (FirstName IS NULL
       OR trim(FirstName) = ''
       OR upper(FirstName)
        IN ('NA', 'N/A', 'UNKNOWN'));
```


**Requirements**.

To create a sigma rule:

  - The head is a relational functor with arguments
  - The neck is `:-`{.delightql .sigil}
  - The body consists of conjoined sigma predicates
  - Each parameter must appear at least once in the body
  - In disjunctive form, each clause must reference at least one parameter

> Sigma predicates include:
>
> - Infix comparisons: `age < 20`, `LastName = 'Johnson'`
> - Functor predicates: `+like(description, 'ipod%')`, `+between(Salary, 50000, 100000)`
> - `in` statements: `state in ("MA"; "TX"; "CA")`
> - Existence tests: `+other_table(...)`, `\+other_table(...)`
