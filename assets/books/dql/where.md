# Where {.dqlh}


Selection (σ in Codd's relational algebra, `WHERE` in SQL) filters rows without
changing schema. [SQL's use of "select" for projection is unfortunate -- Codd used
"select" for row filtering. Delightql follows Codd's terminology.]{.sidenote}

Delightql uses the comma (conjunction) to attach predicates to relations. This is the
same syntax as joins and comes directly from Prolog.

## Domain Predicates {.dqlh}


```delightql
employee(*), Salary > 50000
```

```sql
select * from employee where Salary > 50000;
```

Multiple predicates conjoin naturally:

```delightql
  employee(*), Salary > 50000,
    trim:(lower:(Department))="engineering"
```

```sql
select * from employee
  where Salary > 50000
    and trim(lower(Department))
      IS NOT DISTINCT FROM 'engineering';
```

**Scope restricts commutativity**. Predicates can only reference columns already in scope. This is invalid:

```{.delightql .bad}
// WONT WORK because Salary is not yet in scope
  Salary > 50000,
    trim:(lower:(Department))="engineering",
    employee(*)
```

But once columns are in scope, predicates may be reordered:


```delightql
employee(*),
  Salary > 50000,
  trim:(lower:(Department))="engineering"

// commutativity allowed when all LVars are in scope

employee(*),
  trim:(lower:(Department))="engineering",
  Salary > 50000
```


**Null-safe vs Null-dangerous equality**.  Delightql reserves the `=`{.delightql .sigil}
sigil for the SQL comparison operator `IS NOT DISTINCT FROM`{.sql}.  To use the
traditional (dangerous) equality in SQL, use delightql's `==`{.delightql .sigil} sigil.

```delightql
employee(*), Salary > 50000,
    trim:(lower:(Department))="engineering",
    LastName=="John"
```

```sql
select * from employee
  where Salary > 50000
    and trim(lower(Department))
      IS NOT DISTINCT FROM 'engineering'
    and LastName='John';
```

> **Three-Valued Logic**
>
>
> Null has been with databases since the very beginning and so has the debate
> about its semantics and danger.
>
> SQL provides 'good enough' semantics for its usage in the set operations of
> distinct, grouping, union and intersect, but it can be a foot-gun in
> other circumstances.
>
> The simplest display of its behavior below:
>
> ```sql
> select
>     null=null,
>     null is null,
>     null is not null,
>     1=null,
>     1 is null,
>     1 is not null,
>     1 in (select null union all select 2),
>     1 not in (select null union select 2),
>     1 in (select null union all select 1),
>     1 not in (select null union select 1)
> ;
> ```
>
> shows many odd results
>
> ```
> null=null                              =  null
> null is null                           =  1
> null is not null                       =  0
> 1=null                                 =  null
> 1 is null                              =  0
> 1 is not null                          =  1
> 1 in (select null union all select 2)  =  null
> 1 not in (select null union select 2)  =  null
> 1 in (select null union all select 1)  =  1
> 1 not in (select null union select 1)  =  0
> ```

| Sigil | Name                    | SQL Equivalent         |
|-------|-------------------------|------------------------|
| `=`   | **NULL-SAFE-GROUND-EQ** | `IS NOT DISTINCT FROM` |
| `==`  | **TRAD-GROUND-EQ**      | `=` or `==`            |
| `>`   | **GROUND-GT**           | `>`                    |
| `<`   | **GROUND-LT**           | `<`                    |
| `>=`  | **GROUND-GTE**          | `>=`                   |
| `<=`  | **GROUND-LTE**          | `<=`                   |
| `!=`  | **NULL-SAFE-NOT-EQ**    | `IS DISTINCT FROM`     |
| `!==` | **TRAD-NOT-EQ**         | `!=`                   |
: Infix domain predicates

> **The join-position exception**.
>
> The table above describes equality in *filter position* -- conditions
> referencing columns from zero or one relation. In *join position* --
> conditions correlating columns from two or more relations -- both `=`
> and `==` compile to SQL `=`.
>
> This is the safe default for joins as `IS NOT DISTINCT FROM` in a join
> condition would treat NULL as a matchable value. The NULL-by-NULL cartesian
> product is almost never intended and can explode cardinality.
>
> Joins establish *structural correspondence* -- "these rows belong
> together." NULL means absence, and absence
> does not make a correspondence. Filters test *value equality*, where
> null-safety matters because rows should not silently disappear.
>
> The compiler already distinguishes these contexts: a condition
> referencing two relations becomes an ON clause; a condition referencing
> one relation becomes a WHERE clause. The equality semantics ride on
> this same distinction.
>
> To opt into null-matching joins (the rare case where NULL-to-NULL
> correspondence is desired), use a danger gate:
>
> ```delightql
> employee(*) as e (~~danger://cardinality/nulljoin ON~~),
>   department(*) as d,
>   e.DepartmentId = d.DepartmentId
> ```
>

## Argumentative Grounding {.dqlh}

When using argumentative functor notation, a ground term in argument position induces selection:

```delightql
stock_ownership(1,stock_id,stock_name,quantity)
```

```sql
select
  stock_id,
  stock_name,
  quantity
from stock_ownership where people_id IS NOT DISTINCT FROM 1;
```

**All argumentative grounding uses null-safe equality**.

The grounded column (`people_id  IS NOT DISTINCT FROM  1`) filters rows and is excluded from projection. Multiple grounds compound:

```delightql
stock_ownership(people_id,5,stock_name,120)
```


```sql
SELECT people_id, stock_name
FROM stock_ownership
WHERE
  stock_id IS NOT DISTINCT FROM 5
  AND quantity IS NOT DISTINCT FROM 120;
```

Any domain expression that reduces to a ground term may also be used in argumentative position:

```delightql
stock_ownership(people_id,(4 + 1),upper:("msft"),120)
```

```sql
select
  people_id
from stock_ownership where stock_id IS NOT DISTINCT FROM (4+1) and quantity IS NOT DISTINCT FROM 120 and stock_name IS NOT DISTINCT FROM upper('msft');
```

The Prolog heritage is evident in this syntax and extends to joins -- covered in a later section.

## Semi-Joins and Anti-Joins {.dqlh}

Semi-joins (∃ or ⋉) and anti-joins (∄ or ▷) test for existence without contributing
columns. They ask "can you prove this?" rather than "give me this data."

The **PROVE** sigil `+`{.delightql .sigil} prefixes a semi-join:

```{.delightql .numberLines}
employee(*) as e, +fired_employees(, e.EmployeeId=id)
```


```sql
SELECT *
FROM employee AS e
WHERE
  EXISTS (
    SELECT 1
    FROM fired_employees
    WHERE
      id IS NOT DISTINCT FROM e.EmployeeId
  );
```


The DISPROVE sigil `\+`{.delightql .sigil} prefixes an anti-join: [This syntax comes directly from
Prolog's negation-as-failure.]{.sidenote}

```delightql
employee(*) as e, \+ fired_employees(, e.EmployeeId=f.id)
```


```sql
select
  *
from employee e
  where not exists (select 1 from fired_employees
                      where id IS NOT DISTINCT FROM e.EmployeeId);
```



The join condition(s) appears *inside* the parentheses -- this is called *interior notation*.
The relation is tested for provability, not joined for data.


## The `in` Predicate {.dqlh}

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


## Relational `in` {.dqlh}

The literal form tests membership in a fixed list. The relational form tests
membership in the result of a query -- SQL's `IN (SELECT ...)`.

The right-hand side is any DQL relation (a table access, a pipe chain, or an
anonymous table):

```delightql
employee(*), DepartmentId in department(|> (DepartmentId))
```

```sql
SELECT * FROM employee
  WHERE DepartmentId IN (SELECT DepartmentId FROM department);
```

When the relation already has exactly one column, projection is unnecessary:

```delightql
employee(*), State in valid_states(*)
```

```sql
SELECT * FROM employee
  WHERE State IN (SELECT State FROM valid_states);
```



### Tuple relational `in` {.dqlh}

Multi-column matching extends the tuple `in` syntax (`(x,y) in (1,2;3,4)`)
to relations. The relation must produce exactly as many columns as the
left-hand tuple:

```delightql
employee(*), (State, Department) in valid_combos(|> (State, Department))
```

```sql
SELECT * FROM employee
  WHERE (State, Department) IN
    (SELECT State, Department FROM valid_combos);
```


### Negation: `not in` {.dqlh}

```delightql
employee(*), DepartmentId not in terminated_depts(|> (DepartmentId))
```

```sql
SELECT * FROM employee
  WHERE DepartmentId NOT IN (SELECT DepartmentId FROM terminated_depts);
```


> **Arity rule**
>
> The relation must produce exactly as many columns as the left side has
> elements -- one for a scalar, *N* for an *N*-tuple.  A mismatch is a
> compile-time error.

> **Relation to semi-joins**
>
> Relational `in` is syntactic sugar over the semi-join notation introduced
> [above](#semi-joins-and-anti-joins).
> `col in R(|> (c))` desugars to `+R(, col = c)`;
> `col not in R(|> (c))` desugars to `\+R(, col = c)`.


## Inverted `In` {.dqlh}

The anonymous semi-join syntax permits an inversion -- ground the header, vary the rows:


```delightql
people(*),
    +_("MA" @
      birth_state;
      death_state;
      work_state;
      marriage_state)
```


```sql
select
  *
from people
  where birth_state IS NOT DISTINCT FROM 'MA'
    or death_state IS NOT DISTINCT FROM 'MA'
    or work_state IS NOT DISTINCT FROM 'MA'
    or marriage_state IS NOT DISTINCT FROM 'MA';
```


This asks: "does 'MA' appear in any of these columns?" The columns become the
rows of the anonymous table; the constant becomes the match target.

>   **SQL supports Inverted In**
>
>  Though it might be a revelation to some -- including the author! --
>  the inverted in is standard SQL and is fully supported by all dialects:
>
>   ```sql
>   select
>     *
>   from people
>     where 'MA' in
>       (birth_state,death_state,work_state,marriage_state);
>   ```


Similarly, to test if one column equals any of several others:


```delightql
people(*), +_(birth_state @ death_state; work_state; marriage_state)
```

```sql
select
  *
from people
  where birth_state IS NOT DISTINCT FROM death_state
    or birth_state IS NOT DISTINCT FROM work_state
    or birth_state IS NOT DISTINCT FROM marriage_state;
```

## Sigma Predicates {.dqlh}

Predicates can be defined and reused. A *sigma predicate* is a rule that expands
into selection criteria: [Defining sigma predicates is covered in
DDL.]{.sidenote}

```{.delightql .numberLines .am}
 no_data("NA";"N/A";"UNKNOWN")

 empty(column) :- null=column
 empty(column) :- trim:(column)==""
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


### *`Like`* and `Between` {.dqlh}


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


### Disjunction {.dqlh}

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
