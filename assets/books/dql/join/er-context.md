
# ER-Context Joins {.dqlh}

When join relationships are defined via **ER-context-rules** (see DDL), the `&` and `&&` operators provide concise join syntax:
```delightql
  users(*) & orders(*)
```

Equivalent to:
```delightql
users(*), orders(*), users.id = orders.user_id
```

The `&` operator performs direct lookup; `&&` finds a path through the ER-graph:
```delightql
users(*) && items(*)
// Compiler finds: users -> orders -> items
```

ER-context joins compose with all other features -- filters, projections, aggregations, additional explicit joins.

For defining ER-rules and contexts, see **DDL: ER-Context Rules**.
