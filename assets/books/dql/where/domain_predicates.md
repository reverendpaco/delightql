# Domain Predicates {.dqlh}


```delightql
invoice(*), total > 10
```

```sql
select * from invoice where total > 10;
```

Multiple predicates conjoin naturally:

```delightql
  invoice(*), total > 10,
    trim:(lower:(billing_country))="usa"
```

```sql
select * from invoice
  where total > 10
    and trim(lower(billing_country))
      IS NOT DISTINCT FROM 'usa';
```

**Scope restricts commutativity**. Predicates can only reference columns already in scope. This is invalid:

```{.delightql .bad}
// WONT WORK because Total is not yet in scope
  customer(*),
    total > 10,
    trim:(lower:(billing_country))="usa",
    invoice(*)
```

But once columns are in scope, predicates may be reordered:


```delightql
invoice(*),
  total > 10,
  trim:(lower:(billing_country))="usa"

// commutativity allowed when all LVars are in scope

invoice(*),
  trim:(lower:(billing_country))="usa",
  total > 10
```


**Null-safe vs Null-dangerous equality**.  Delightql reserves the `=`{.delightql .sigil}
sigil for the SQL comparison operator `IS NOT DISTINCT FROM`{.sql}.  To use the
traditional (dangerous) equality in SQL, use `+sql_eq(x,y)`.

```delightql
invoice(*), total > 10,
    trim:(lower:(billing_country))="usa",
    +sql_eq(billing_state,"CA")
```

```sql
select * from invoice
  where total > 10
    and trim(lower(billing_country))
      IS NOT DISTINCT FROM 'usa'
    and billing_state='CA';
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
> ```text
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
Likewise, for SQL's `!=` there is `+sql_ne(left,right)`

> **The row-correspondence exception**.
>
> The table above describes local value tests -- conditions comparing values
> within one row or against a ground term. An ordinary comparison matching
> two relation row occurrences uses SQL `=`, whether the matches feed a
> multiplying join, an EXISTS/NOT EXISTS, or a correlated scalar subquery.
>
> `IS NOT DISTINCT FROM` in correspondence would treat NULL as a matchable
> value and can explode cardinality. Suppressing the extra rows with EXISTS
> does not make that correspondence correct.
>
> Joins establish *structural correspondence* -- "these rows belong
> together." NULL means absence, and absence
> does not make a correspondence. Filters test *value equality*, where
> null-safety matters because rows should not silently disappear.
>
> SQL clause placement is not the equality rule: a correlated EXISTS often
> has an inner WHERE, and still matches two row occurrences. A local
> `inner.x = null` beside its correlation stays null-safe.
>
> There is no `cardinality/nulljoin` gate. If an absent key has a real
> matchable meaning, first map both sides to an explicit non-NULL key and
> match those keys. The NULL itself never establishes correspondence.
>
