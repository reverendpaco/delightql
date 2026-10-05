# Common Higher-order Table Expressions {.dqlh}

Higher-order expressions may be used as common expressions.

```delightql
above(amt,I(*))(*) : I(*), $.amt<total
below(amt,I(*))(*) : I(*), $.amt>total
mark_as(l,I(*))(*) : I(*) ~> count:(*) as count |> +($.l as label)
invoice(*)
  |> above(1)(*)
  |> below(5)(*)
  : one_to_five
invoice(*)
  |> above(5)(*)
  |> below(10)(*)
  : five_to_ten
invoice(*)
  |> above(10)(*)
  : ten_up

one_to_five(|>mark_as("one_to_five")(*)) ;
ten_up(|>mark_as("ten_up")(*)) ;
five_to_ten(|>mark_as("five_to_ten")(*))
```

## Recursion {.dqlh}

A common higher-order expression may reference itself. Its head declares
the name before any clause body is read, so a clause can call the
expression it belongs to. A self-reference with the same arguments is a
fixpoint, exactly as it is for the same rule written in a rules file: the
clauses without a self-reference are the anchor, the others are applied to
the previous iteration's rows until none are produced, and the clauses
accumulate by `UNION ALL`.

```delightql
reports_to(boss)(employee_id) : employee(*), employee_id = $.boss
reports_to(boss)(employee_id) :
    employee(*) as e,
    reports_to($.boss)(*) as r,
    e.reports_to = r.employee_id
    |> (e.employee_id)
reports_to(2)(*)
```

The argument selects one fixpoint and stays fixed while it runs:
`reports_to(2)` inside its own body reads the rows found so far for
employee 2. The rules of recursion are the ones in the Recursion in Rules
chapter, whichever form the definition is written in:

- A base clause comes first. A self-reference before any base clause
  refuses as `semantic/recursion/anchor_first`.
- A self-reference with a different argument refuses as
  `semantic/recursion/parameter-widening`. State that changes from step to
  step belongs in the relation's columns, as `v` does below.
- A clause references the expression at most once
  (`semantic/recursion/nonlinear`).

The argument may be any value, including a column of the caller's rows,
and the expression may be handed to another rule as a rule value; it
recurses the same way in every position.

```delightql
n:(x | (x % 2) = 0) : x / 2
n:(x) : (3 * x) + 1
collatz(s)(*) : _(v @ $.s)
collatz(s)(*) : collatz($.s)(*), v != 1 |> (n:(v) as v)
collatz(10)(*)
```

The badge `%` after the name makes the clauses accumulate by `UNION`
instead, as it does on a rule: a row already found is not found again, so
a traversal over a cycle ends. Every clause wears the same badge.

```delightql
edge(*) : _(src, dst @ "a", "b"; "b", "a"; "b", "c")
reach%(s)(node) : _(node @ $.s)
reach%(s)(node) :
    reach($.s)(*),
    edge(*) as e,
    e.src = node
    |> (e.dst as node)
reach("a")(*)
```
