# Higher-Order Rules {.dqlh}

Higher-order rules accept tables or scalars as parameters. SQL calls these
table-valued functions; Prolog would call them higher-order predicates.

## Syntax {.dqlh}

A higher-order rule is very easy to see. It has two sets of parentheses: the first for parameters -- input values, the second
for output values.

```delightql
department_employee_count(E(*), D(*))(department, employee_count) :-
  E(*), D(*.(DepartmentId))
    |> %(D.department ~> count:(*) as employee_count)
```

In the above example, the parameters `E(*)` and `D(*)` are *inner glob functors* -- they accept
whatever tables are passed at invocation.  The `(*)` signals that the
body will reference these tables' columns by name.

## Direct Invocation {.dqlh}

As discussed in the section on DQL, tables can be
passed in directly:

```delightql
department_employee_count(employee_2019(*), department_2019(*))(*)
```

The call site mirrors the definition head: each table parameter is a
full functor expression.  This makes the call self-documenting -- you
can see which arguments are tables and which are scalars without
looking up the definition.

Because call-site arguments are relation expressions, they can compose:

```delightql
department_employee_count(
  employee_2019(*, Salary > 50000),
  department_2019(*)
)(*)
```

Here the first argument is a filtered relation -- only high earners
are counted.

## Piped Invocation {.dqlh}

Pipes can be used on any higher-order predicate that takes
a table-valued parameter:

```delightql
clean_employees(T(*))(*) :-
  T(*) as t
    |> $(trim:())(t.LastName, t.FirstName)
    |> $(to_iso:())(t.BirthDate, t.HireDate)
```

```delightql
employee_2019(*)
  |> clean_employees(*)
```

The piped relation fills the first parameter.  The `(*)` after the
rule name is the output schema, not an input functor.

Chaining is possible:

```delightql
mask_ssn(T(*), mask_value)(*) :-
  T(*) |> $$(mask_value as ssn)
```

```delightql
employee_2019(*)
  |;| employee_2018(*)
  |;| employee_2017(*)
  |> clean_employees(*)
  |> mask_ssn("***-**-****")(*)
```

**Note**. As with function pipes, the relation is piped into the first parameter
of the higher-order predicate.  If the higher-order predicate has multiple
parameters, the other values must be set.

**Multi-parameter piped invocation.** When the piped relation is not the first
parameter, use `@` (the f-param placeholder) to mark where it goes -- the same
syntax as function pipes:

```delightql
-- Definition: scalar first, table second
tagged(label, T(*))(*) :- T(*), ...

-- Direct invocation (always works):
tagged("young", users(*))(*)

-- Piped invocation with @:
users(*) |> tagged("young", @)(*)
```

The `@` tells the compiler which parameter receives the piped relation.
Without `@`, the piped relation fills the first parameter by default --
which fails when the first parameter is a scalar.

## Scalar Parameters {.dqlh}

A bare identifier without parentheses is a scalar parameter.  It binds
a single value used directly in body expressions:

```delightql
high_earners(T(*), salary_floor, min_count)(*) :-
  T(*), Salary > salary_floor,
    department(*.(DepartmentId))
    |> %(department ~> count:(*) as employee_count),
    employee_count > min_count
```

```delightql
high_earners(employee(*), 50000, 10)(*)
```

Scalar parameters accept single values only -- not tables, not pipes,
not multi-row inline data.  Functor expressions are visually distinct
from scalar literals, so comma separation is unambiguous.



# Inner Functors {.dqlh}

Higher-order parameters come in four flavors, distinguished by **syntax** in
the definition head:

| Form      | Kind                      | Name                        |
|-----------|---------------------------|-----------------------------|
| `T(*)`    | table, structurally typed | inner glob functor          |
| `T(a, b)` | table, positionally typed | inner argumentative functor |
| `n`       | scalar value              | scalar parameter            |
| `f:()`    | function value            | function parameter          |

The syntax alone tells the language what each parameter accepts.
Capitalization is conventional -- programmers *should* uppercase table
parameters and lowercase scalars for readability, but the language
does not require it.

## Inner Glob Functors {.dqlh}

An **inner glob functor** `T(*)` is **structurally typed**: the body
references columns by name, and any table that has those columns is
accepted regardless of width.

```delightql
clean_employees(T(*))(*) :-
  T(*) as t
    |> $(trim:())(t.LastName, t.FirstName)
    |> $(to_iso:())(t.BirthDate, t.HireDate)
```

The parameter `T(*)` accepts any table with `LastName`, `FirstName`,
`BirthDate`, and `HireDate` columns -- it may have other columns too.
This is duck typing: if it has the right columns, it fits.


## Inner Argumentative Functors {.dqlh}

An **inner argumentative functor** `T(a, b)` is **positionally typed**:
the input must have exactly two columns, and they are renamed to `a` and
`b` inside the body.  The caller's original column names are overwritten.

```delightql
foo(T(label, value))(*) :-
  T(*), value > 10 |> (label)
```

The names `label` and `value` are column aliases available in the body.
The definition simultaneously declares the arity (two columns) and
provides names for positional access.

The advantage of the argumentative functor is in the calling convention,
called *scalar lifting*.  Because the definition declares a positional
contract, a call site *may* pass bare scalars instead of a table:

```delightql
foo("first", 2)(*)
```

The scalars are positionally matched to the declared columns `label` and
`value` and lifted into a one-row table.  This cascades to stacked notation:

```delightql
foo("first", 2; "second", 20)(*)
```

which sugars explicit anonymous tables:

```delightql
foo(_("first", 2; "second", 20))(*)
```

but still allows pipe invocation:

```delightql
two_column_table(*)
  |> foo(*)
```

Or explicit functor invocation:

```delightql
foo(two_column_table(*))(*)
```

Scalar lifting requires a positional contract -- an inner glob functor
cannot accept inline scalars because there is no declared arity to match
against.



## The `&` Rule {.dqlh}

**`&` is required only when using scalar lifting with an argumentative
functor alongside other parameters.**

When every table argument is passed as a functor expression, the
parentheses disambiguate each argument. Commas separate parameters
as usual -- no `&` needed.  `&` is the cost of the scalar-lifting
shorthand: when bare scalars fill an argumentative functor, `&`
marks where one argument ends and the next begins.

:::{.widen}
| Definition                   | Functor call site                     | `&`?               |    |
|------------------------------|---------------------------------------|--------------------|----|
| `f(T(*), V(*))`              | `f(users(*), orders(*))(*)`           | no                 |    |
| `f(T(*), V(*))`              | `users(*)                             | > f(orders(*))(*)` | no |
| `f(T(*), n)`                 | `f(users(*), 10)(*)`                  | no                 |    |
| `f(T(*), V(*), n)`           | `f(users(*), orders(*), 10)(*)`       | no                 |    |
|                              |                                       |                    |    |
| `f(T(x, y))`                 | `f(data(col1, col2))(*)`              | no                 |    |
| `f(T(x, y))`                 | `f(_(1, 2; 10, 20))(*)`               | no                 |    |
| `f(T(x, y))`                 | `_(1, 2; 10, 20)                      | > f(*)`            | no |
|                              |                                       |                    |    |
| `f(T(*), V(x, y))`           | `f(users(*), _(1, 2))(*)`             | no                 |    |
| `f(T(*), V(x, y))`           | `f(users(*), data(col1, col2))(*)`    | no                 |    |
| `f(T(*), V(x, y))`           | `users(*)                             | > f(_(1, 2))(*)`   | no |
|                              |                                       |                    |    |
| `f(T(*), V(x, y), n)`        | `f(users(*), _(1, 2), 10)(*)`         | no                 |    |
| `f(T(*), V(x, y), n)`        | `f(users(*), _(1, 2; 10, 20), 10)(*)` | no                 |    |
|                              |                                       |                    |    |
| `f(::ns, n, V(x, y))`        | `f(data::prod, "t", _(1, 2))(*)`      | no                 |    |
|                              |                                       |                    |    |
| *Scalar lifting (shorthand)* |                                       |                    |    |
|                              |                                       |                    |    |
| `f(T(x, y))` *(single)*      | `f(1, 2)(*)`                          | no                 |    |
| `f(T(x, y))`                 | `f(1, 2; 10, 20)(*)`                  | no                 |    |
|                              |                                       |                    |    |
| `f(T(*), V(x, y))`           | `f(users(*) & 1, 2)(*)`               | yes                |    |
| `f(T(*), V(x, y))`           | `f(users(*) & 1, 2; 10, 20)(*)`       | yes                |    |
|                              |                                       |                    |    |
| `f(T(x, y), n)`              | `f("a", "b" & 10)(*)`                 | yes                |    |
| `f(T(x, y), V(a, b))`        | `f(1, 2 & 3, 4)(*)`                   | yes                |    |
|                              |                                       |                    |    |
| `f(::ns, n, V(x, y))`        | `f(data::prod & "t" & 1, 2)(*)`       | yes                |    |
::::

The table is divided into two regions.  In the top region, every table
argument uses functor syntax -- `&` is never needed.  In the bottom
region (scalar lifting), bare scalars fill argumentative functors and
`&` marks the boundaries.

Functor syntax is always available and always unambiguous. Scalar
lifting is an optional shorthand for inline data -- use `&` when you
use it, or wrap in `_()` to avoid it.


## Parameter Grounding and Multi-Clause HO Entities {.dqlh}

The two parentheses partition a single relation into "positions the
caller sets" and "columns the body produces." Both sets follow the
same head semantics (see [Head Semantics](#head-semantics)), applied
independently.

**First parentheses: always argumentative.** The parameter
parentheses must explicitly declare parameters.

**Second parentheses: any head form.** The output parentheses follow
all standard head rules: glob or argumentative, consistent across
clauses, smart union corresponding for globs, strict positional+name
agreement for argumentative.

### Parameter modes {.dqlh}

Table parameters are input-moded -- they must be grounded because
the body computes over them. There is no way to enumerate "all
tables." Scalar parameters are bidirectional -- they can be grounded
(filter to matching clauses) or left free (project as a column).

| Parameter kind | Mode          |
|----------------|---------------|
| Scalar         | bidirectional |
| Table          | input-only    |


A scalar parameter can be left free
(unbound at the call site) only when every clause has a ground term at that
position.

```delightql
schema("employees")(name, type) :-
  _(name, type ---- "id", "INT"; "name", "TEXT")
schema("departments")(name, type) :-
  _(name, type ---- "dept_id", "INT"; "dept_name", "TEXT")
```

The ground constants define the enumeration domain:
`schema(entity)(*)` enumerates `"employees"` and `"departments"` at the parameter position
because those are the constants in the clauses.

If any clause has a free variable at that position, the caller must provide a concrete value --
either a literal or a variable bound from context (a pipe, CTE,
join, etc.).

### Scalar parameters as discriminators {.dqlh}

When a higher-order entity has multiple clauses with different ground scalar
values in a parameter position, invocation with a matching ground
term selects the relevant clauses. This is clause selection via
equality -- the same mechanism as argumentative grounding.

```delightql
schema("employees")(name, type) :-
  _(name, type ---- "id", "INT"; "name", "TEXT")
schema("departments")(name, type) :-
  _(name, type ---- "dept_id", "INT"; "dept_name", "TEXT")
```

Grounded query -- filter:

```delightql
schema("employees")(*)
```

The ground term `"employees"` selects the matching clause.
Output: `(name, type)` -- two columns. The ground parameter
filters but does not project.

Free query -- project:

```delightql
schema(entity)(*)
```

The first position is free. All clauses contribute (UNION ALL).
The free parameter appears as a column:

```
entity      | name     | type
------------|----------|--------
employees   | id       | INT
employees   | name     | TEXT
departments | dept_id  | INT
departments | dept_name| TEXT
```

This is the same behavior as argumentative grounding on regular
relations where `stock_ownership(1, stock_id, ...)` filters on position 1
and drops it from the output.


### Mixed ground/free across clauses {.dqlh}

The same scalar position can be ground in some clauses and free
in others. This follows standard relational semantics: every
clause that matches contributes rows.

```delightql
foo("a", y)(*) :- ...   -- pos 1 ground, pos 2 free (parameter)
foo("b", y)(*) :- ...   -- pos 1 ground, pos 2 free (parameter)
foo(x, "c")(*) :- ...   -- pos 1 free (parameter), pos 2 ground
```

Invocation:

```delightql
_(z ---- "c"; "d"; "e") |> foo("a", z)(*)
```

For each row, both positions are concrete. Each clause either
matches or doesn't:

- Clause 1: `"a"` matches `"a"`, `y` binds to `z`. **Selected.**
- Clause 2: `"a"` doesn't match `"b"`. **Excluded.**
- Clause 3: `x` binds to `"a"`, `z` must equal `"c"`.
  **Selected for that row only.**

There is no ambiguous dispatch. The relational world has no
"which clause wins?" conflict -- every matching clause contributes
rows.
