# ER-Context Rules {.dqlh}

ER-context rules allow a programmer to  pre-define entity relationships via join conditions.


ER-context rules define the join relationships once. The `&`{.delightql .sigil}
and `&&`{.delightql .sigil} operators reference them concisely.

## Defining Relationships {.dqlh}

An ER-rule declares how two tables join. The head uses `&`{.delightql .sigil}
between table names; the body is the join expression:

```delightql
users(*) & orders(*) :-
  users(*), orders(*), users.id = orders.user_id

users(*) & items(*) :-
  orders(*), items(*), orders.id = items.order_id

items(*) & products(*) :-
  items(*), products(*), items.product_id = products.id
```

The `&` alone assign this join within a context called `::normal`.

## Multiple Contexts {.dqlh}

The same table pair can have different join semantics in different contexts:

```delightql
users(*) & orders(*) :-
  users(*), orders(*), users.id = orders.user_id

users(*) &(::audit) orders(*) :-
  users(*), orders(*), users.id = orders.created_by

orders(*) &(::audit) audit_log(*) :-
  orders(*), audit_log(*), orders.id = audit_log.order_id
```

The context name is a symbol, i.e. a `::` followed by a valid identifier.
The lack of a symbol means `::normal`.  The following are the same:

```delightql
users(*) & orders(*) :-  // body
users(*) &(::normal) orders(*) :-  // body
```

## Using Contexts {.dqlh}

Calling the join mirrors the way in which the rules was defined:

```delightql
users(*) & orders(*)

users(*) &(::audit) orders(*)
```

## Direct Join (`&`{.delightql .sigil}) {.dqlh}

The `&`{.delightql .sigil} operator performs a direct lookup in the current context:

```delightql
users(*) & orders(*)
```

Equivalent to:
```delightql
users(*), orders(*), users.id = orders.user_id
```

Multiple `&`{.delightql .sigil} operators chain left-to-right. Each consecutive pair must have a defined ER-rule:

```delightql
users(*) & orders(*) & items(*)
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
users(*) && products(*)
```

No direct `users(*)&products(*)` rule exists, but the path does: `users -> orders ->
items -> products`.

**Ambiguity is an error.** If multiple paths exist, the query fails:

```delightql
users(*)&orders(*) :- ...
orders(*)&items(*) :- ...
users(*)&items(*) :- ...   // creates a cycle

users(*) && items(*)
// Error: Ambiguous join path from 'users' to 'items':
//   Path 1: users -> orders -> items
//   Path 2: users -> items (direct)
```
