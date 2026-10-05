# ER-Context Rules {.dqlh}

ER-context rules allow a programmer to  pre-define entity relationships via join conditions.


ER-context rules define the join relationships once. The `&`{.delightql .sigil}
and `&&`{.delightql .sigil} operators reference them concisely.

## Defining Relationships {.dqlh}

An ER-rule declares how two tables join. The head uses `&`{.delightql .sigil}
between table names; the body is the join expression:

```{.delightql .am}
customer(*) & invoice(*) :-
  customer(*), invoice(*), customer.customer_id = invoice.customer_id

invoice(*) & invoice_line(*) :-
  invoice(*), invoice_line(*), invoice.invoice_id = invoice_line.invoice_id

invoice_line(*) & track(*) :-
  invoice_line(*), track(*), invoice_line.track_id = track.track_id
```

The `&` alone assigns this join to the default context `::normal`.

## Multiple Contexts {.dqlh}

The same table pair can have different join semantics in different contexts:

```{.delightql .am}
customer(*) &(::support) employee(*) :-
  customer(*), employee(*), customer.support_rep_id = employee.employee_id

customer(*) &(::local) employee(*) :-
  customer(*), employee(*), customer.city = employee.city
```

The context name is a symbol, i.e. a `::` followed by a valid identifier.
The lack of a symbol means `::normal`.  The following are the same:

```delightql
customer(*) & invoice(*)
customer(*) &(::normal) invoice(*)
```

## Using Contexts {.dqlh}

Calling the join mirrors the way in which the rule was defined:

```delightql
customer(*) & invoice(*)

customer(*) &(::local) employee(*)
```

## Direct Join (`&`{.delightql .sigil}) {.dqlh}

The `&`{.delightql .sigil} operator performs a direct lookup in the written
context, or `::normal` when omitted. It does not search every context:

```delightql
customer(*) & invoice(*)
```

Equivalent to:
```delightql
customer(*), invoice(*), customer.customer_id = invoice.customer_id
```

Multiple `&`{.delightql .sigil} operators chain left to right. Each consecutive pair must have a defined ER-rule:

```delightql
customer(*) & invoice(*) & invoice_line(*)
```

Compiles to:
```delightql
customer(*), invoice(*), invoice_line(*),
  customer.customer_id = invoice.customer_id,
  invoice.invoice_id = invoice_line.invoice_id
```

## Transitive Join (`&&`) {.dqlh}

The `&&` operator finds a path through the ER-graph:

```delightql
customer(*) && track(*)
```

No direct `customer(*)&track(*)` rule exists, but the path does: `customer -> invoice ->
invoice_line -> track`.

**Ambiguity is an error.** If multiple paths exist, the query fails:

```{.delightql .am}
employee(*) &(::sales) customer(*) :-
  employee(*), customer(*), employee.employee_id = customer.support_rep_id
customer(*) &(::sales) invoice(*) :-
  customer(*), invoice(*), customer.customer_id = invoice.customer_id
employee(*) &(::sales) invoice(*) :-   // creates a cycle
  employee(*), invoice(*), employee.city = invoice.billing_city
```

```{.delightql .bad}
employee(*) &&(::sales) invoice(*)
// Error: Ambiguous: 2 paths from 'employee(*)' to 'invoice(*)':
//   employee(*) -> invoice(*)
//   employee(*) -> customer(*) -> invoice(*)
```
