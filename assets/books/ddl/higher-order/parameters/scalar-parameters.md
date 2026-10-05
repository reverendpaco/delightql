
# Scalar Parameters {.dqlh}

A bare identifier without parentheses in the head declares a scalar
parameter. The body reads it as `$.name`: the sigil `$.` glued to the
parameter's name. A bare name in the body is always a column.

```{.delightql .am}
big_markets(T(*), total_floor, min_count)(*) :-
  T(*), total > $.total_floor,
    customer(*.(customer_id))
    |> %(country ~> count:(*) as invoice_count),
    invoice_count > $.min_count
```

```delightql
big_markets(invoice(*), 10, 4)(*)
```

## Parameters and columns do not compete {.dqlh}

Because a parameter is always marked, a parameter and a column may share a
name. Adding a column to an input can never redirect a reference to the
parameter, and renaming a parameter never changes which column a bare name
reads:

```delightql
above(total, T(*))(*) : T(*), total > $.total
above(20, invoice(*))(*)        // invoices whose own Total column exceeds 20
```

A pipe changes which columns are in view, never what `$.total` reads:

```{.delightql .am}
h(total, T(*))(*) :-
  T(*) |> ((unit_price * quantity) as total) |> (total, $.total as supplied)
```

Here the final `total` is the column the previous stage created, and
`$.total` is the parameter.

## Every named parameter is read {.dqlh}

A family must use every scalar parameter it names. A position counts as
used when some clause reads it as `$.name`, or when some clause grounds it
with a constant in its head. Otherwise the definition refuses where it is
declared (`semantic/ddl/head/unused_scalar`):

```{.delightql .am .bad}
h(total, T(*))(*) :- T(*), total > 20      // refused: `Total` here is a column, and
                                           // the parameter `Total` is read by no clause
```

## Clause-local names {.dqlh}

Scalar formal names are local to each clause. For example, `h(k)(*)` with
body `_(v @ $.k)` and `h(j)(*)` with body `_(v @ $.j + 100)` bind the same
argument position. Calling `h(7)(*)` returns 7 and 107 in either clause
order. The same positional binding applies to query-local rules and effect
rules. Family metadata labels this input `argument 1`; neither local name
is the family's parameter name. Public output-head agreement is unchanged.

## Where `$.name` stands {.dqlh}

`$.name` is a value. It stands wherever a value stands, including:

- a slot, where it constrains the column rather than binding one:
  `T($.k, v)`;
- an argument forwarded to another rule: `inner($.k, T)(*)`;
- a bound or an ordinal, which take the parameter's literal actual:
  `T(*), #< $.n` and `|$.n|`.

It never names or addresses a column, so it cannot follow `as`, stand in a
selector, or appear in a head.

## Nested definitions {.dqlh}

A definition written inside a higher-order body may read the enclosing
rule's parameters. `$.name` searches the enclosing higher-order clauses,
nearest first. A nested higher-order rule's own parameter of the same name
shadows the outer one. Each invocation of the outer rule hands its nested
definitions its own actuals:

```{.delightql .am}
outer(limit)(*) :-
    upto(s)(v) : _(v @ $.s)
    upto(s)(v) : upto($.s)(*), v < $.limit |> (v + 1 as v)
    upto(1)(*)
```

Value functions, contextual functions, truth rules and lambdas keep bare
parameter names, and they do not interrupt this search: a nested value
function may combine its own bare `x` with an enclosing `$.limit`. A `$.name`
that no enclosing higher-order clause declares refuses where it is written
(`semantic/resolution/parameter`).
