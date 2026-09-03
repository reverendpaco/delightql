# Assertions {.dqlh}

Assertions are ordinary effects. They apply a pure property rule to the
relation at the authored point, abort the current run when the property
answers NO, and otherwise release the exact relation that was checked.

```delightql
at_least(n, T(*))(*) : T(*) ~> count:(*) as count, count >= n

users(*), age < 20
  !> assert!(at_least(2), "at least two young users")(*)
  |> (id, email)
```

The property argument is a closed rule value with exactly one relation input
left. Configuration such as `at_least(2)` is resolved when the value is
closed. Query-scoped and consulted properties retain their lexical identity;
`assert!` does not look their authored spelling up again.

The label is optional:

```delightql
has_rows(T(*))(*) : T(*) |> `exists`(*)
users(*) !> assert!(has_rows(*))(*)
```

An omitted label receives a stable synthetic occurrence label. A direct call
places the checked relation last, because a pipe fills the final formal:

```delightql
assert!(has_rows(*), "users exist", users(*))(*)
```

## Result and failure {.dqlh}

A nonempty property result is a witness and passes. An empty property result
reaches the fundamental `abort!` effect with the identity
`runtime/assertion`. Failure stops later effects in that run and rolls the
current transaction back; work committed by an earlier `;` run remains.
Errors evaluating either the checked relation or its property keep their
original identities and are not relabelled as assertion failures.

On success the receipt has:

| column | meaning |
|---|---|
| `success` | `1` |
| `operation` | `"assert!"` |
| `label` | authored or synthetic label |
| `witnesses` | the exact property-result occurrence used for the verdict |
| `returned` | the exact checked input occurrence |

The unwrap pipe `!>` releases `returned`. The implementation establishes the
input once and the witness once, so volatile inputs cannot be checked as one
occurrence and returned as another.

## Common properties {.dqlh}

The standard prelude includes pure property rules such as `exists`,
`notexists`, `count_is(n)`, `at_least(n)`, and `same_bag(expected)`.

```delightql
orders(*) !> assert!(count_is(3), "three orders")(*)

actual(*)
  !> assert!(same_bag(expected(*)), "expected rows")(*)
```

User properties can express filters, aggregates, schema meta-relations, or
other pure relational checks. Effectful rules, scalar callables, unknown
target callables, extra holes, and incompatible residual headings refuse at
the ordinary rule-value boundary.

## Observability {.dqlh}

Passing and failing verdicts reach host assertion hooks and `sys.assertions`.
The failure row is recorded outside the target transaction so it remains
queryable after rollback and the session can continue.

The former `(~~assert … ~~)` annotation has been removed. It now receives a
retirement diagnostic directing programs to a property rule and `assert!`;
there is no compatibility sidecar or separately executed assertion SQL.
