# Unification and Logical Variables {.dqlh}

Delightql inherits from Prolog a simple rule: **identifiers unify
when their names match exactly**.  To **unify** means
to insist on equality, i.e. the two variables at
different locations must refer to the same datum. Unlike Prolog, however, delightql's identifiers are
qualified by their table names.

Delightql also uses Prolog's argumentative unification
syntax, where the position of an argument in a functor
unifies whatever is written in this place: a ground, or another LVar
with the dimension accessed at that location.

## How Names Are Introduced {.dqlh}

The way you access a table determines the names of its columns in scope:

| Access Pattern | Columns Introduced |
|----------------|-------------------|
| `users(id, name, status, _)` | `id`, `name`, `status` |
| `users(*)` | `users.id`, `users.name`, `users.status` |
| `users(*) as u` | `u.id`, `u.name`, `u.status` |

: Columns introduced by access pattern

Argumentative access introduces **unqualified** names -- bare identifiers. Wildcard access introduces **qualified** names -- prefixed by table name or alias.


## Unification Creates Joins {.dqlh}

When the same name appears in multiple places, unification creates a join condition:
```delightql
artist(artist_id, name), album(album_id, title, artist_id)
```

Both introduce `artist_id`. Unification produces:
```sql
SELECT artist.artist_id, artist.name, album.album_id, album.title
FROM artist, album
WHERE artist.artist_id = album.artist_id;
```


## Wildcard Access and Qualification {.dqlh}
```delightql
artist(*), album(*)
```

This introduces `artist.artist_id` and `album.artist_id` -- different names. No unification occurs; the result is a cross join.

To join with wildcard access, use explicit conditions:
```delightql
artist(*), album(*), artist.artist_id = album.artist_id
```

Or use the USING operator `.(cols)`:
```delightql
artist(*), album(*.(artist_id))
```


## Qualified References in Argumentative Access {.dqlh}

Argumentative patterns can reference lvars from other tables:
```delightql
artist(*) as a, album(album_id, title, a.artist_id)
```

The `a.artist_id` in positional access matches the `a.artist_id` from `artist(*) as a`, creating unification. This mixes styles: wildcard for one table, positional for another, with explicit cross-reference.

A more elaborate example:
```delightql
playlist(*) as p,
track(*) as t,
playlist_track(p.playlist_id, t.track_id)
```

Here `playlist_track` unifies with `playlist` on `p.playlist_id` and with `track` on `t.track_id` -- a three-way join through positional cross-references.

## Literals and Constraints {.dqlh}

Ground terms in positional access create `WHERE` conditions:

```delightql
invoice(invoice_id, customer_id, invoice_date, _, _, _, "Canada", _, total)
```

The positional grounding filters rows where the seventh column equals `"Canada"`.  The column `billing_country` has been unified with the ground value `"Canada"`.

```sql
SELECT invoice_id, customer_id, invoice_date, total FROM invoice WHERE billing_country IS NOT DISTINCT FROM 'Canada';
```

## Self-Unification {.dqlh}

The same name repeated in positional access forces equality:
```delightql
invoice(invoice_id, customer_id, invoice_date, _, billing_city, billing_city, _, _, total)
```

Columns 5 and 6 both bind to `billing_city`. This filters to rows where those columns are equal:
```sql
SELECT invoice_id, customer_id, invoice_date, billing_city, total FROM invoice WHERE billing_city IS NOT DISTINCT FROM billing_state;
```

## Anonymous Tables and Unification {.dqlh}

Anonymous tables participate in unification through their header names:
```delightql
invoice(invoice_id, customer_id, _, _, _, _, billing_country, _, total),
_(billing_country @ "Canada"; "France"; "Germany")
```

The anonymous table introduces `billing_country`. This matches `billing_country` from `invoice`, creating:
```sql
SELECT invoice_id, customer_id, billing_country, total
FROM invoice
WHERE billing_country IN ('Canada', 'France', 'Germany');
```

With wildcard access, qualification is required:
```delightql
invoice(*) as i,
_(i.billing_country @ "Canada"; "France"; "Germany")
```

Without the `i.` prefix, no unification occurs -- the anonymous table's `billing_country` wouldn't match `i.billing_country`.

## Lvars as Data in Anonymous Tables {.dqlh}

Anonymous tables can use lvars as data values, not just in headers.

**Constraint:** An lvar cannot appear both in a header and in the data rows of the same anonymous table.

### Inverted IN Pattern {.dqlh}
```delightql
employee(*) as e,
_(2 @ e.employee_id; e.reports_to)
```

Find employees where `2` appears in any of these columns:
```sql
SELECT * FROM employee e
WHERE 2 IN (e.employee_id, e.reports_to);
```

Or equivalently:
```sql
SELECT * FROM employee e
WHERE e.employee_id = 2
   OR e.reports_to = 2;
```

The anonymous table's header is a literal (`2`); the data rows are lvars from `employee`. This inverts the typical IN pattern.

### EAV Transformation {.dqlh}

```delightql
employee(*) as e,
_(attribute, value @
  "name", e.last_name;
  "email", e.email;
  "title", e.title;
  "hired", e.hire_date)
```

This is the melt pattern.

### Row-Wise Correspondence {.dqlh}
```delightql
customer(*) as c,
employee(*) as e,
_(c.country, e.employee_id @
  e.country, c.support_rep_id;
  e.country, 1;
  "USA", 2)
```

Each row in the anonymous table represents a valid combination. The result includes only rows where `(c.country, e.employee_id)` matches one of the specified pairs:
```sql
SELECT *
FROM customer c, employee e
WHERE (c.country = e.country AND e.employee_id = c.support_rep_id)
   OR (c.country = e.country AND e.employee_id = 1)
   OR (c.country = 'USA' AND e.employee_id = 2);
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
