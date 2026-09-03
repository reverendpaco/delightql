

# Moding and currying {.dqlh}

Moding is the activity of marking a column as input only, output only, or both.
Another way of saying *input* and *output* is *supplied* and *returned*.

The term *mode* comes from Prolog where syntax exists to declare certain dimensions to be
input and others to be output. [[ In ISO this syntax is documentation only.
Some Prolog implementations enforce these annotations.]{.sidenote}]{.sidenote-number}

```prolog
:- mode concatenate(+,+,-).
```

We must be aware of this Prolog heritage as we discuss
where delightql matches versus diverges from it.

## Prolog syntax {.dqlh}

In Prolog, all predicates obey the same syntax (except for infix) and
declare their dimensions.

```prolog
foo(A,B,C).
```

But each column could be used in queries as output only, input only, or mixed (and possibly functional). Respectively:

All output:
```prolog
foo(A,B,C).
```

All input:
```prolog
foo(1,"bar",42).
```

Some input, some output:
```prolog
foo(A,"bar",C).
```

In a very important way, the below is **also** an example of input only:

```prolog
foo(A,"bar",C),A=1,C=42.
```

The logic variables `A` and `C` in the example above are still being input, as they have been instantiated with ground values.

## Delightql's answer to moding {.dqlh}

Delightql has semantic enforced definitions of which
columns must be input only, versus those that can be
mixed input or output.

Succinctly:

  - All ordinary function parameters are input-only.
  - Every position in an ordinary table or relational-rule schema is relational/bidirectional: it may be supplied or returned.
  - All sigma-predicate parameters are input-only.
  - A fact function **with** a `_` default (**TOTAL-CASE**) arm:
    - positions left of `->` are input-only
    - and positions right of `->` are output-only.
  - A fact function **without** the default arm (**PARTIAL-CASE**) is table-like: all columns may be input or output **when accessed like a table** (e.g. `foo(a,b,c)`)
  - A fact function **without** the default arm (**PARTIAL-CASE**) can also be accessed like a function, in which case all ouput columns are elided and only the leftmost input columns are written (e.g.  `foo:(a,b)` where `a,b -> c`).
  - A fact-function call with exactly one output may stand directly in scalar/value composition; this implicitly selects its sole output. The output may still be selected explicitly by name.
  - A fact-function call with several outputs must name the exact output selected whenever a scalar value is required..
  - All parameters in the first parentheses of a higher-order/input-moded rule are input-only

All of the above semantics are **parsable**, that is to say, they are all different syntaxes that should be easily discernible.



