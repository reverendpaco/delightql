
# Sigma Predicates {.dqlh}

Predicates can be defined and reused. A *sigma predicate* is a rule that expands
into selection criteria: [Defining sigma predicates is covered in
DDL.]{.sidenote}

```{.delightql .numberLines .am}
 no_data("NA";"N/A";"UNKNOWN")

 empty(column) :- null=column
 empty(column) :- trim:(column)=""
 empty(column) :- +no_data(upper:(column))
```

Use with semi-join or anti-join syntax:[Delightql applies De Morgan's laws,
distributing negation across disjunctive clauses.]{.sidenote}

```delightql
employee(*),
  \+empty(LastName),
  \+empty(FirstName)
```


```sql
SELECT *
FROM employee
WHERE
  LastName IS NOT null
  AND '' != trim(LastName)
  AND upper(LastName) NOT IN (
    'NA',
    'N/ A',
    'UNKNOWN'
  )
  AND (FirstName IS NOT null
  AND '' != trim(FirstName)
  AND upper(FirstName) NOT IN (
    'NA',
    'N/ A',
    'UNKNOWN'
  ));
```


## *`Like`* and `Between` {.dqlh}


SQL's `LIKE` and `BETWEEN` have special syntax. Delightql maps functor notation to these constructs:

```delightql
employee(*), +like(Email,"%.com"), \+between(Salary,10000,100000)
```

The above delightql transpiles to the following Sql.

```sql
select
  *
from employee
  where
    Email like '%.com' and
    Salary not between 10000 and 100000;
```


## Disjunction {.dqlh}

Two syntaxes express `OR`:

**Keyword form (recommended)**. The `or` keyword binds predicates within a sigma clause:

```delightql
employee(*)
  , trim:(lower:(Department)) = "executive"
        or Salary > 120000
  , Title != "Engineer"
  |> %( DepartmentCity
          ~>
        count:(*) as employee_count,
        avg:(Salary) )
```


**Sigil form**. The **SEMI-OR** sigil `;`{.delightql .sigil} requires parentheses to capture scope:


```delightql
employee(*)
  , (trim:(lower:(Department)) = "executive"
        ; Salary > 120000 )
  , Title != "Engineer"
  |> %( DepartmentCity
          ~>
        count:(*) as employee_count,
        avg:(Salary) )
```

Prefer the keyword form -- it reads more clearly and avoids parenthesis errors.
See "Precedence and Scoping" for details.
