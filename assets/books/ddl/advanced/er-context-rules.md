# ER-Context Rules {.dqlh}

ER-context rules are an answer to the question:  what if we could
define entity relationships that inform queries?

Normalized schemas encode relationships with foreign keys but query expressions
do not take advantage of this. SQL requires repeating join conditions in every
query -- unaware of DDL constraints.


ER-context rules define these relationships once. The `&`{.delightql .sigil}
and `&&`{.delightql .sigil} operators reference them concisely.

## Defining Relationships {.dqlh}

An ER-rule declares how two tables join. The head uses `&`{.delightql .sigil}
between table names; the body is the join expression:

```delightql
users&orders(*) within normal :-
  users(*), orders(*), users.id = orders.user_id

orders&items(*) within normal :-
  orders(*), items(*), orders.id = items.order_id

items&products(*) within normal :-
  items(*), products(*), items.product_id = products.id
```

The `within` clause assigns the rule to a named context.

## Multiple Contexts {.dqlh}

The same table pair can have different join semantics in different contexts:

```delightql
users&orders(*) within normal :-
  users(*), orders(*), users.id = orders.user_id

users&orders(*) within audit :-
  users(*), orders(*), users.id = orders.created_by

orders&audit_log(*) within audit :-
  orders(*), audit_log(*), orders.id = audit_log.order_id
```


The context name is any valid identifier.

## Using Contexts {.dqlh}

The `under` directive activates a context. It must be the first token in the query:

```delightql
under normal: users(*) & orders(*)

under audit: users(*) & orders(*)
```

The directive applies to the entire query scope. Mixing contexts in one query is not permitted.

## Direct Join (`&`{.delightql .sigil}) {.dqlh}

The `&`{.delightql .sigil} operator performs a direct lookup in the current context:

```delightql
under normal: users(*) & orders(*)
```

Equivalent to:
```delightql
users(*), orders(*), users.id = orders.user_id
```

Multiple `&`{.delightql .sigil} operators chain left-to-right. Each consecutive pair must have a defined ER-rule:

```delightql
under normal: users(*) & orders(*) & items(*)
```

Compiles to:
```delightql
users(*), orders(*), items(*),
  users.id = orders.user_id,
  orders.id = items.order_id
```

## Transitive Join (`&&`) {.dqlh}

The `&&` operator finds a path through the ER-graph:

```delightql
under normal: users(*) && products(*)
```

No direct `users&products` rule exists, but the path does: `users -> orders ->
items -> products`.

**Ambiguity is an error.** If multiple paths exist, the query fails:

```delightql
users&orders(*) within normal :- ...
orders&items(*) within normal :- ...
users&items(*) within normal :- ...   // creates a cycle

under normal: users(*) && items(*)
// Error: Ambiguous join path from 'users' to 'items':
//   Path 1: users -> orders -> items
//   Path 2: users -> items (direct)
```
