# Domain Predicates {.dqlh}


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
| `==`  | does NOT exist          |                        |
| `>`   | **GROUND-GT**           | `>`                    |
| `<`   | **GROUND-LT**           | `<`                    |
| `>=`  | **GROUND-GTE**          | `>=`                   |
| `<=`  | **GROUND-LTE**          | `<=`                   |
| `!=`  | **NULL-SAFE-NOT-EQ**    | `IS DISTINCT FROM`     |
: Infix domain predicates

If a programmer requires the use of the traditional Sql `=`
they can use the named functor: `+sql_eq(left,right)`.
Likewise, for SQL's `!=` there is `+sql_not_eq(left,right)`

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
