
# Default Transpilation Strategy and Resolution {.dqlh}

Delightql syntax makes a clear distinction between what is

  - a table -- `foo(*)`{.delightql}
  - a higher-order table -- `foo(bar)(*)`{.delightql},
  - a function -- `foo:(x,y)`{.delightql}, or
  - a sigma predicate -- `+foo(bar)(*)`{.delightql}.

These different categories of *entity* have different rules for both resolution
and transpilation.  Importantly, functions and higher-order tables among these are
permitted to be unknown and transpiled directly as SQL fragments. Tables and
sigma predicates **must** be known.

## Table Resolution and Transpilation {.dqlh}

Calls of the form `foo(*)` may either be delightql
rules or target tables.

```delightql
foo(*)
```

In either case, delightql's resolution both ensures that the name (`foo`)
exists and that its columns (dimensions) are named and in a certain order.  For
a data-backed table, the default transpilation rule places the entity after a
`FROM` (e.g. `FROM foo`).

```sql
select * from foo;
```

Resolution for non-namespaced entities checks common-table expressions first followed by any in-scope
entities.  Only CTEs are allowed to shadow -- no named entities sharing the same name
are allowed to exist in the same scope.


## Higher-order Resolution and Transpilation {.dqlh}

```delightql
foo(bar)(*)
```

Higher-order forms can be delightql rules or target table-value functions.
If delightql finds the entity as a delightql-authored higher-order rules, it
will use the rule as needed.

But if a delightql-authored higher-order rule is not found it will **default**
to assuming that the entity is a table-value function and transpile as
a function-call form after the `FROM`.

```sql
select * from foo(bar);
```

This **default tranpsilation rule** runs assuming imperfect knowledge as to
whether the SQL target defines such an entity.


## Function Resolution and Transpilation {.dqlh}

Functions may be delightql-authored or functions hosted by the target SQL engine. If
delightql finds an entity that is delightql-authored, it will use it first inclusive
of shadowing.

Upon finding no delightql-authored function, delightql will emit a SQL fragment that transpiles the delightql
function form into the SQL function form:

```delightql
foo(*) |> +( bar:(x) as b)
```

```sql
select *, bar(x) as b from foo;
```

This default tranpsilation rules runs assuming imperfect knowledge as to
whether the SQL target defines such an entity. That is to say, the `bar`
in the above SQL may or may not exist as a function in the target SQL
engine.


## Sigma Predicate Resolution and Transpilation {.dqlh}

All sigma-predicates have **no** default transpilation rule.
This is because SQL has no uniform syntax for row predication with
such diverse syntax as `LIKE`, `BETWEEN`, in-fix operators, `IN`, and `EXSISTS`.

All delightql calls of the form

```delightql
foo(*), +bar(x)
```

will resolve to a delightql-authored entity (either built-in or programmer authored).
