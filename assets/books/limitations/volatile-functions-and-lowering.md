# Non-deterministic functions {.dqlh}

> Delightql makes **NO** guarantee about impure function-level referential
> transparency or delimited referential transparency.  The rest of this section
> details the challenges that will need to be overcome to permit any future
> guarantees.
>
> True referential transparency is a strong guarantee: that a function with
> the exact same arguments may be substituted with the one value it produces
> in all situations.
>
> Delimited referential transparency is a looser guarantee:  that **within one
> calling context**, a function form with the exact same arguments may be
> substituted with the one value it produces.


Non-deterministic functions like certain vendors' `random()` may not have
clearly defined semantics around delimited referential transparency.

To motivate,  in SQLite SQL, a random value across many rows:

```sql
with
  _x as ( VALUES (1), (2), (3))
select random() as r from _x;
```
| r |
|---|
| 7906891248351165381  |
| -8883546532970986158  |
| -8728216203524003274  |

feels correct,
as does when calling random twice in the same expression:

```sql
with
  _x as ( VALUES (1), (2), (3))
select random() as r1, random() as r2 from _x;
```

r1 | r2
---|----
-3037396489422509295|4469562710814311768
-4848443231643320494|-713724063462585786
4226627867170606774|2825360043875777743

When we desire our expected flavor of delimited referential transparency, the random
function "materialized" once seems to work like we think it should:

```sql
with
  _random as (
    select random() as r
      union all
    select random() as r)
select
    r,
    r+1 as r_plus_one
  from _random;
```

```sql
select
    r,
    r+1 as r_plus_one
  from
  (
    select random() as r
      union all
    select random() as r)
```

r | r_plus_one
---|----
-7433712953842994257|-7433712953842994256
-8813118922048968177|-8813118922048968176

but when SQLite's rules about flattening
mark the expression differently, we may
not get what we think we are going to get:

```sql
WITH input(n) AS (
  VALUES (1), (2), (3)
)
SELECT r AS first_read, r AS second_read
FROM (
  SELECT random() AS r
  FROM input
);
```

first_read | second_read
--- | ----
1747592606472746190|-5537291275306372640
-8262209585999104522|6666755709977980449
6154489567650109788|-8039489511224155566

SQLite does, however, adhere to a certain form of referential transparency
for those non-deterministic functions that are part of the
SQL standard:  `CURRENT_TIME`,`CURRENT_DATE`,`CURRENT_TIMESTAMP`.

The difference in this behaviour can be understood
by the taxonomic [ Postgres terminology ](https://www.postgresql.org/docs/current/xfunc-volatility.html):

 - **IMMUTABLE**: a function is guaranteed to be referentially transparent in all situations
 - **STABLE**: a function is guaranteed to be referentially transparent within one **context**
 - **VOLATILE**: a function is not guaranteed to be referentially transparent

> NOTE: the term "context" may mean different things between different SQL
> targets: for some, at the unit of a query, for others at the unit of a
> transaction.

## Delightql's challenge {.dqlh}

Delightql has several challenges in this corner of the SQL world:

 - is there a semantics that can be guaranteed equivalent across SQL targets?
 - can the standard semantic of "one delightql query expression = one SQL query expression" be maintained?
 - is the IMMUTABLE/STABLE/VOLATILE taxonomy a suitable _standard_ for coloring SQL functions?
 - can rewrite equivalences match _any_ expected semantics?

## Cross-target  {.dqlh}

The SQLite example above shows that its implementations of the  functions `random()` and `current_timestamp()`
are VOLATILE and STABLE respectively.  That the function `current_timestamp` is part of the SQL standard
implies that SQL vendors should maintain STABLE semantics for these.

A  matrix of implementation strategies across vendors shows that even the STABLE semantic comes
with some vendor-specific caveats:

::::{.widen}
| SQL-standard family            | PostgreSQL                          | DuckDB                                   | MySQL                                                              | SQL Server                                            |
|--------------------------------|-------------------------------------|------------------------------------------|--------------------------------------------------------------------|-------------------------------------------------------|
| `CURRENT_DATE`                 | Yes; transaction start              | Yes; transaction start                   | Yes; query start                                                   | SQL Server 2025+/Azure                                |
| `CURRENT_TIME`                 | Yes; transaction start              | Yes; transaction start                   | Yes; query start                                                   | No ordinary T-SQL spelling                            |
| `CURRENT_TIMESTAMP`            | Yes; transaction start              | Yes; transaction start                   | Yes; query start                                                   | Yes; classified nondeterministic                      |
| `LOCALTIME`                    | Yes; transaction start              | Yes; transaction start                   | Accepted as a `NOW()` synonym, not the standard `TIME` distinction | No                                                    |
| `LOCALTIMESTAMP`               | Yes; transaction start              | Yes; transaction start                   | Accepted as a `NOW()` synonym                                      | No                                                    |
| `NEXT VALUE FOR`               | No; uses `nextval()`                | No; uses `nextval()`                     | No sequences                                                       | Yes                                                   |
| `PREVIOUS VALUE FOR`           | No; uses `currval()`                | No; uses `currval()`                     | No                                                                 | No                                                    |
| Routine volatility declaration | `IMMUTABLE` / `STABLE` / `VOLATILE` | UDF `side_effects` metadata              | `DETERMINISTIC` / `NOT DETERMINISTIC`, author-asserted             | Engine-derived determinism and schema binding         |
:::::

The challenge therefore may be answered with vendor-specific transpilation using correct transactional boundaries.


## Query-only Semantics and Materialization {.dqlh}

**The primary semantic** that delightql guarantees is that all pure queries are
equivalent to the SQL string that they produce.  That delightql does
not have a direct mapping between query string to query string when directives are
utilized is not a contradiction:  directives are impure and are marked as such (`!`).

Therefore if delightql wishes to support support referential transparency
for functions syntactically marked as pure:

```delightql
_(a@1;2;3) as anon
    |> +(random:() as r)
    |> (r, r+1)
```

it cannot result in an admixture of DDL and SQL:

```sql
BEGIN;

CREATE TEMPORARY TABLE __dql_stage AS
WITH input(a) AS (
  VALUES (1), (2), (3), (3)
)
SELECT
  input.a,
  random() AS r
FROM input;

SELECT
  a,
  r,
  r + 1
FROM __dql_stage;

DROP TABLE __dql_stage;

COMMIT;
```

If delightql eventually does allow for these impure functions,
it will require marking them as impure:


```{.delightql .bad}
_(a@1;2;3) as anon
    |> +(random!:() as r)
    |> (r, r+1)
```

which will

 - give a syntactic indication to the programmer that both the primary semantic
is void and that a new (to-be-determined) referential semantic is now in force
 - instruct the delightql compiler to create an effect plan of DDL and transactions

This remains a challenge under the current epistemic regime whereby unknown functions
are default transpiled into the target SQL:

```{.delightql .bad}
employee(*) |> +( do_it:(last_name) as di)
```

```sql
select *, do_it(last_name) as di from employee;
```

## The IMMUTABLE/STABLE/VOLATILE {.dqlh}

## Relational Equational Rewriting {.dqlh}
