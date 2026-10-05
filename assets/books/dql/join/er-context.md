
# ER-Context Joins {.dqlh}

When join relationships are defined via **ER-context-rules** (see DDL):
```{.delightql .am}
customer(*) & invoice(*) :-
  customer(*), invoice(*), customer.customer_id = invoice.customer_id

invoice(*) & invoice_line(*) :-
  invoice(*), invoice_line(*), invoice.invoice_id = invoice_line.invoice_id

invoice_line(*) & track(*) :-
  invoice_line(*), track(*), invoice_line.track_id = track.track_id
```

the `&` and `&&` operators provide concise join syntax:
```delightql
  customer(*) & invoice(*)
```

Equivalent to:
```delightql
customer(*), invoice(*), customer.customer_id = invoice.customer_id
```

The `&` operator performs direct lookup; `&&` finds a path through the ER-graph:
```delightql
customer(*) && track(*)
// Compiler finds: customer -> invoice -> invoice_line -> track
```

ER-context joins compose with all other features -- filters, projections, aggregations, additional explicit joins.

For defining ER-rules and contexts, see **DDL: ER-Context Rules**.
