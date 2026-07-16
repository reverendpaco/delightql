# Unification and Logical Variables {.dqlh}

Delightql inherits from Prolog a simple rule: **identifiers unify
when their names match exactly**.  To **unify** means
to insist on equality and via such a simple semantic
we can provide an alternate form to joining and filtering.

Unlike Prolog, however, delightql's identifiers are
qualified by their table names.

## How Names Are Introduced {.dqlh}

The way you access a table determines the names of its columns in scope:

| Access Pattern | Columns Introduced |
|----------------|-------------------|
| `users(id, name, status, _)` | `id`, `name`, `status` |
| `users(*)` | `users.id`, `users.name`, `users.status` |
| `users(*) as u` | `u.id`, `u.name`, `u.status` |

: Columns introduced by access pattern

Argumentative access introduces **unqualified** names -- bare identifiers. Wildcard access introduces **qualified** names -- prefixed by table name or alias.

This distinction is the source of most unification behavior.

## Unification Creates Joins {.dqlh}

When the same name appears in multiple places, unification creates a join condition:
```delightql
users(user_id, name, _), orders(order_id, user_id, total, _)
```

Both introduce `user_id`. Unification produces:
```sql
SELECT users.name, orders.order_id, orders.total
FROM users, orders
WHERE users.user_id = orders.user_id;
```

No explicit join condition is needed here -- the shared name insists on it. This is argumentative joining, covered in the Join chapter.

## Wildcard Access and Qualification {.dqlh}
```delightql
users(*), orders(*)
```

This introduces `users.user_id` and `orders.user_id` -- different names. No unification occurs; the result is a cross join.

To join with wildcard access, use explicit conditions:
```delightql
users(*), orders(*), users.user_id = orders.user_id
```

Or use the USING operator `.(cols)`:
```delightql
users(*), orders(*.(user_id))
```


## Qualified References in Argumentative Access {.dqlh}

Argumentative patterns can reference lvars from other tables:
```delightql
users(*) as u, orders(order_id, u.user_id, total, _)
```

The `u.user_id` in positional access matches the `u.user_id` from `users(*) as u`, creating unification. This mixes styles: wildcard for one table, positional for another, with explicit cross-reference.

A more elaborate example:
```delightql
users(*) as u,
reviews(*) as r,
products(product_id, u.user_id, r.rating, _)
```

Here `products` unifies with `users` on `u.user_id` and with `reviews` on `r.rating` -- a three-way join through positional cross-references.

## Literals and Constraints {.dqlh}

Ground terms in positional access create `WHERE` conditions:

```delightql
users(user_id, name, "active", _)
```

The positional grounding filters rows where the third column equals `"active"`.  The column `status` has been unified with the ground value `"active"`.

```sql
SELECT user_id, name FROM users WHERE status = 'active';
```

## Self-Unification {.dqlh}

The same name repeated in positional access forces equality:
```delightql
users(user_id, name, user_id, _)
```

Columns 1 and 3 both bind to `user_id`. This filters to rows where those columns are equal:
```sql
SELECT user_id, name FROM users WHERE column1 = column3;
```

## Anonymous Tables and Unification {.dqlh}

Anonymous tables participate in unification through their header names:
```delightql
users(user_id, name, status, _),
_(status @ "active"; "pending"; "suspended")
```

The anonymous table introduces `status`. This matches `status` from users, creating:
```sql
SELECT user_id, name, status
FROM users
WHERE status IN ('active', 'pending', 'suspended');
```

With wildcard access, qualification is required:
```delightql
users(*) as u,
_(u.status @ "active"; "pending"; "suspended")
```

Without the `u.` prefix, no unification occurs -- the anonymous table's `status` wouldn't match `u.status`.

## Lvars as Data in Anonymous Tables {.dqlh}

Anonymous tables can use lvars as data values, not just in headers.

**Constraint:** An lvar cannot appear both in a header and in the data rows of the same anonymous table.

### Inverted IN Pattern {.dqlh}
```delightql
users(*) as u,
_("happy" @ u.status; u.feelings; u.worldview)
```

Find users where `"happy"` appears in any of these columns:
```sql
SELECT * FROM users u
WHERE 'happy' IN (u.status, u.feelings, u.worldview);
```

Or equivalently:
```sql
SELECT * FROM users u
WHERE u.status = 'happy'
   OR u.feelings = 'happy'
   OR u.worldview = 'happy';
```

The anonymous table's header is a literal (`"happy"`); the data rows are lvars from `users`. This inverts the typical IN pattern.

### EAV Transformation {.dqlh}

```delightql
users(*) as u,
_(attribute, value @
  "name", u.name;
  "email", u.email;
  "status", u.status;
  "created", u.created_at)
```

This is the melt pattern discussed in a later chapter.

### Row-Wise Correspondence {.dqlh}
```delightql
users(*) as u,
orders(*) as o,
_(u.status, o.priority @
  u.feelings, o.urgency;
  u.mood, "high";
  "active", "rush")
```

Each row in the anonymous table represents a valid combination. The result includes only rows where `(u.status, o.priority)` matches one of the specified pairs:
```sql
SELECT *
FROM users u, orders o
WHERE (u.status = u.feelings AND o.priority = o.urgency)
   OR (u.status = u.mood AND o.priority = 'high')
   OR (u.status = 'active' AND o.priority = 'rush');
```


## Summary of Unification Rules {.dqlh}

| Pattern             | Names Introduced            | Unifies With                             |
|---------------------|-----------------------------|------------------------------------------|
| `t(a, b, c)`        | `a`, `b`, `c`               | Any `a`, `b`, `c`                        |
| `t(*)`              | `t.a`, `t.b`, `t.c`         | Only `t.a`, `t.b`, `t.c`                 |
| `t(*) as x`         | `x.a`, `x.b`, `x.c`         | Only `x.a`, `x.b`, `x.c`                 |
| `t(x.a, b, _)`      | `x.a`, `b`                  | `x.a` from alias `x`; any `b`            |
| `_("lit" @ v1; v2)` | (none -- header is literal) | Filters where `lit` matches `v1` or `v2` |
| `_(col @ "a"; "b")` | `col`                       | Any `col`                                |

: Summary of unification rules
