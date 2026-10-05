# Recursion in Rules {.dqlh}

Recursion in delightql emerges from self-reference. When a predicate's
definition includes a clause that references the predicate itself, the
definition is recursive. When a common table expression includes a clause that
references the CTE itself, the CTE is recursive. Both transpile to SQL's `WITH
RECURSIVE` construct.

This chapter covers the semantics of recursion in delightql, how it maps to
SQL's execution model, and the constraints that model imposes.

## Two Forms of Recursion {.dqlh}

Delightql supports recursion in two contexts:

**Recursive rules** are defined in assertion mode and persist as reusable predicates:

```{.delightql .am}
superior(employee, boss) :-
  employee(*), reports_to != null
    |> (employee_id as employee, reports_to as boss)
superior(employee, boss) :-
  employee(*) as e, superior(*) as s, e.reports_to = s.employee
    |> (e.employee_id as employee, s.boss)
```

**Recursive CTEs** are defined inline in query mode, scoped to a single query:

```delightql
superior(*) : employee(*), reports_to != null |> (employee_id as employee, reports_to as boss)
superior(*) : employee(*) as e, superior(*) as s, e.reports_to = s.employee
    |> (e.employee_id as employee, s.boss)
superior(*)
```

Both forms transpile to `WITH RECURSIVE`. The choice depends on whether the recursive logic is reusable (rule) or ad-hoc (CTE).

A parameterized rule may be written in either place: in a rules file with
`:-`, or in the query itself with `:` as a common higher-order expression.
The two recurse identically; see Higher-Order Recursive Predicates below.

## The Anatomy of Recursion {.dqlh}

Every recursive definition has two components:

**Base clauses** provide initial rows without self-reference. These are SQL's "anchor members":

```delightql
_(n @ 1) : counter                                 // literal base case
counter(*)

employee(*), title = "General Manager" : mgmt      // filtered base case
mgmt(*)

employee(*), reports_to != null                     // projected base case
    |> (reports_to as origin, employee_id as dest) : reachable
reachable(*)
```

**Recursive clauses** reference the predicate or CTE being defined. These are SQL's "recursive members":

```delightql
_(n @ 1) : counter                                 // base case
counter(*), n < 100 |> (n + 1 as n) : counter
counter(*)

employee(*), title = "General Manager" : mgmt      // base case
mgmt(*) as m, employee(*) as o, o.reports_to = m.employee_id |> (o.*) : mgmt
mgmt(*)

employee(*), reports_to != null                     // base case
    |> (reports_to as origin, employee_id as dest) : reachable
reachable(*) as r, employee(*) as e, r.dest = e.reports_to
    |> (r.origin, e.employee_id as dest) : reachable
reachable(*)
```


Base clauses must precede recursive clauses in source order: names
resolve strictly left to right, and a recursive clause reads a name the
base clause establishes. In a recursive CTE the base clause creates the
name, so writing the recursive clause first refuses as an unresolved name
("Table not found"). A rule, or a parameterized rule written in the query,
declares its name in its head, so the name can see itself; there a
self-reference before any base clause refuses as
`semantic/recursion/anchor_first`.

## Evaluation Model {.dqlh}

SQL's recursive CTEs evaluate using a **working table** algorithm:

1. Execute all base clauses; their results form the initial working table
2. Execute the recursive clause with the working table as input
3. The output becomes the new working table
4. Repeat until the working table is empty
5. Return the union of all iterations

This is **bottom-up** or **co-recursive** evaluation: starting from known facts it derives new facts and repeats until a fixed point is reached. It resembles dynamic programming more than classical recursion.

The critical implication: **the recursive clause sees only the previous iteration's rows, not the full accumulated result**. This is why certain operations are prohibited -- they would require access to rows that haven't been computed yet or have already been consumed.


## What Recursion Can Express {.dqlh}

SQL's recursive model handles a well-defined class of problems:

**Hierarchical traversal** -- org charts, bill of materials, folder structures:

```delightql
employee(*), reports_to = null |> (employee_id, last_name, 0 as depth) : tree
employee(*) as e, tree(*) as t, e.reports_to = t.employee_id
    |> (e.employee_id, e.last_name, t.depth + 1 as depth) : tree
tree(*)
```

**Transitive closure** -- reachability, ancestry, dependency graphs:

```delightql
employee(*), reports_to != null |> (reports_to as origin, employee_id as dest) : reachable
reachable(*) as r, employee(*) as e, r.dest = e.reports_to
    |> (r.origin, e.employee_id as dest) : reachable
reachable(*) |> %(*)  // deduplicate
```

**Sequence generation** -- numeric ranges, date series, iteration:

```delightql
_(d @ date:("2024-01-01")) : dates
dates(*), d < date:("2024-12-31")
    |> (date:(d, "+1 day") as d) : dates
dates(*)
```

**Iterative computation** -- any algorithm expressible as "given previous state, compute next state":

```delightql
_(iter, x, target @ 0, 1.0, 2.0) : newton
newton(*), abs:((x * x) - target) > 0.0001, iter < 100
    |> (iter + 1 as iter, (x + (target / x)) / 2.0 as x, target) : newton
newton(*) |> %(target ~> (x as sqrt) <~ #(iter desc))
```

## What Recursion Cannot Express {.dqlh}

The working-table model imposes fundamental limitations. These are not
arbitrary restrictions -- they follow from the evaluation semantics.

### No Aggregation in Recursive Clauses {.dqlh}

Aggregation requires access to multiple rows. The recursive clause sees only the working table (previous iteration), not the full accumulated result.

```{.delightql .bad}
// INVALID -- cannot aggregate within recursion
similar_artist(*), artist_id = 22    // Led Zeppelin
    |> (similar_id as artist_id, score as strength) : reach
reach(*) as r, similar_artist(*) as s, s.artist_id = r.artist_id, r.strength > 0.5
    |> (s.similar_id as artist_id, r.strength * s.score as strength) : reach  // seems ok?

// But this fails:
reach(*) as r, similar_artist(*) as s, s.artist_id = r.artist_id
    |> %(s.similar_id as artist_id ~> sum:(r.strength * s.score) as strength) : reach  // aggregation -- NOT ALLOWED
reach(*)
```


### No Subqueries Referencing the Recursive Target {.dqlh}

A subquery inside the recursive clause cannot reference the CTE being defined:

```{.delightql .bad}
// INVALID -- subquery references 'paths'
employee(*), reports_to != null |> (reports_to as origin, employee_id as dest) : paths
employee(*) as e, paths(*) as p, e.reports_to = p.dest,
    \+ paths(*, e.employee_id = dest)  // "dest not already reached" -- NOT ALLOWED
    |> (p.origin, e.employee_id as dest) : paths
paths(*)
```

The subquery `paths(*, ...)` would need to see all accumulated rows, which aren't available.


### No Mutual Recursion {.dqlh}

Two predicates cannot reference each other:

```{.delightql .am .bad}
// INVALID -- mutual recursion
even(0)
even(n) :- odd(m), n = m + 1
odd(n) :- even(m), n = m + 1
```

SQL's `WITH RECURSIVE` processes one CTE at a time. There's no mechanism for two CTEs to co-evolve.

The refusal is `semantic/recursion/mutual`, and its message names the
whole cycle (`home::even -> home::odd -> home::even`). A definition that
returns to itself through any other definition — a rule that uses a view
whose body uses the rule — refuses the same way.


### Single Self-Reference {.dqlh}

The recursive clause may reference the target exactly once:

```{.delightql .bad}
// INVALID -- two self-references
employee(*), reports_to != null |> (reports_to as origin, employee_id as dest) : paths
paths(*) as p1, paths(*) as p2, p1.dest = p2.origin
    |> (p1.origin, p2.dest) : paths
paths(*)
```

This would require joining the working table against itself, which SQL doesn't support in recursive CTEs.


### Relations Only {.dqlh}

Recursion is over relations. A value function that calls itself — with
the same argument, a different one, or through other value functions —
refuses as `semantic/recursion/function-form`. A sigma rule that cites
itself, directly or through other sigma rules, refuses as
`semantic/recursion/truth-form`, even with a base clause:

```{.delightql .am .bad}
// INVALID -- a value function has no fixpoint to re-enter
fib:(n) :- fib:(n - 1) + fib:(n - 2)
```

Define the relation and ground the argument instead: `fib(k, v)` as a
recursive relation, `fib(5, v)` to read one point.


## Termination {.dqlh}

Recursive CTEs terminate when the recursive clause produces no new rows. This happens when:

- A `WHERE` condition filters out all candidates
- A join finds no matches
- The depth limit (`#`) is reached
- The data is exhausted (finite traversal)

**Ensuring termination:**

For sequence generation, always include a bound:

```delightql
_(n @ 1) : nums
nums(*), n < 1000 |> (n + 1 as n) : nums  // terminates at 1000
nums(*)
```

For graph traversal over potentially cyclic data, track visited nodes:

```delightql
similar_artist(*)
    |> (artist_id as origin, similar_id as dest, :",{artist_id},{similar_id}," as visited) : paths
similar_artist(*) as e, paths(*) as p, p.dest = e.artist_id,
    \+like(p.visited, :"%,{e.similar_id},%")    // not already on the path
    |> (p.origin, e.similar_id as dest, :"{p.visited}{e.similar_id}," as visited) : paths
paths(*), origin = 22    // from Led Zeppelin: ,22,58,12, is not extended back to 22
```

For unknown depth, use `#` as a safety limit:

```delightql
employee(*), reports_to = null |> (employee_id as id, 0 as depth) : tree
tree(*) as t, employee(*) as n, n.reports_to = t.id, # < 100
    |> (n.employee_id as id, t.depth + 1 as depth) : tree
tree(*)
```


## UNION Badge {.dqlh}

By default, delightql emits `UNION ALL` -- duplicates across iterations are preserved. This is efficient and correct for most traversals.

For graph traversals that require the usage of `UNION` over `UNION ALL` for correctness use
the **UNION-BADGE**  where the badge `%` is postfixed to the name of the rule.

```{.delightql .am}
artist_edge(*) :-
  similar_artist(*)
    |> (artist_id as src, similar_id as dst)

artist_edge(*) :-
  similar_artist(*)
    |> (similar_id as src, artist_id as dst)

connected%(*) :-
  artist(*), name = "Led Zeppelin"
    |> (artist_id)

connected%(*) :-
  connected(*) as c,
    artist_edge(*) as e,
    c.artist_id = e.src
    |> (e.dst as artist_id)
```

This applies to CTEs as well:

```delightql
// Construct an undirected artist graph.
similar_artist(*)
  |> (artist_id as src, similar_id as dst)
  : edge

similar_artist(*)
  |> (similar_id as src, artist_id as dst)
  : edge

// Begin at Led Zeppelin.
artist(*), name = "Led Zeppelin"
  |> (artist_id)
  : connected%

// Follow every edge. UNION prevents previously reached artists
// from returning to the top of the stack
connected(*) as c,
  edge(*) as e,
  c.artist_id = e.src
  |> (e.dst as artist_id)
  : connected%

// Display the connected component.
connected(*) as c,
  artist(*) as a,
  c.artist_id = a.artist_id
  |> (a.artist_id, a.name)
  |> #(artist_id)
```

A parameterized head wears the badge in the same place, after the name
and before the parameter row, in a rules file or in the query:

```{.delightql .am}
connected_to%(root)(*) :-
  artist(*), name = $.root
    |> (artist_id)

connected_to%(root)(*) :-
  connected_to($.root)(*) as c,
    similar_artist(*) as s,
    c.artist_id = s.artist_id
    |> (s.similar_id as artist_id)

connected_to%(root)(*) :-
  connected_to($.root)(*) as c,
    similar_artist(*) as s,
    c.artist_id = s.similar_id
    |> (s.artist_id as artist_id)
```

```delightql
connected_to("Gilberto Gil")(*) as c,
  artist(*) as a,
  c.artist_id = a.artist_id
  |> (a.artist_id, a.name)
  |> #(artist_id)
```

Each argument selects its own fixpoint, and the badge removes duplicates
within that fixpoint only. When the argument comes from a caller's rows,
two caller rows with the same argument each receive the whole result.

Every clause of a definition wears the same badge. A mixed set refuses as
`semantic/recursion/mixed_badge`. A badge on a definition that never
references itself refuses as `semantic/recursion/false_fixpoint`: there is
no fixpoint for it to describe. To remove duplicates from an ordinary
definition, write `|> %(*)` in the body.


## Higher-Order Recursive Predicates {.dqlh}

Recursive rules can be parameterized, deferring the base case:

```{.delightql .am}
reports_to(boss)(employee_id, last_name) :- employee(*), last_name = $.boss
reports_to(boss)(employee_id, last_name) :-
    employee(*) as e,
    reports_to($.boss)(*) as r,
    e.reports_to = r.employee_id
    |> (e.employee_id, e.last_name)
```

Each invocation monomorphizes to a concrete `WITH RECURSIVE`:

```delightql
reports_to("Edwards")(*)   // who reports to Edwards?
reports_to("Mitchell")(*)  // who reports to Mitchell?
```

The higher-order parameter `boss` is inlined into the anchor clause at query time. The recursive structure itself doesn't change -- only the starting point.

The argument selects one fixpoint, and it stays fixed while that fixpoint
runs. The self-reference `reports_to(boss)(*)` passes the same argument,
so it reads the rows found so far. A self-reference that changes an
argument refuses as `semantic/recursion/parameter-widening`; state that
changes from one step to the next belongs in the relation's columns.

The same rule may be written in the query itself, as a common higher-order
expression, with `:` in place of `:-`:

```delightql
reports_to(boss)(employee_id, last_name) : employee(*), last_name = $.boss
reports_to(boss)(employee_id, last_name) :
    employee(*) as e,
    reports_to($.boss)(*) as r,
    e.reports_to = r.employee_id
    |> (e.employee_id, e.last_name)
reports_to("Edwards")(*)
```

It returns the same rows and refuses the same shapes as the rule, in every
position: with a literal argument, with an argument read from the
caller's rows, and handed to another rule as a rule value.

## Example: Mandelbrot Set {.dqlh}

This example demonstrates sequence generation, computational iteration, and post-recursion aggregation working together:

:::::{.widen}
```delightql
_(x@-2.0)                                  : xaxis
xaxis(*), x < 1.2
 |> (x + 0.05 as x)                        : xaxis
_(y@-1.0)                                  : yaxis
yaxis(*), y < 1.0
 |> (y + 0.1 as y)                         : yaxis
sq:(x):
  x * x
xaxis(*), yaxis(*)
 |> (0 as iter,
     x as cx,
     y as cy,
     0.0 as x,
     0.0 as y)                             : m
m(*), (sq:(x) + sq:(y)) < 4.0,
  iter < 28
 |> (iter + 1 as iter,
     cx as cx,
     cy as cy,
     (sq:(x) - sq:(y)) + cx as x,
     ((2.0 * x) * y) + cy as y)            : m
m(*)
 |> %(cx,cy ~> max:(iter) as iter )        : m2
m2(*)
 |> %(cy
        ~>
      group_concat:(substr:(" .+*#", 1+min:(iter/7,4), 1), "") as t)
                                           : a
a(*)
 ~> group_concat:(rtrim:(t),char:(0x0a))
```
::::::

The query generates a coordinate grid, runs the escape-time algorithm via
recursive iteration, then aggregates the results into ASCII art -- all in a
single delightql expression.

## Delightql Recursive Limits {.dqlh}


A true fixed-point engine -- like those in Datalog systems -- would maintain the full set of derived facts and allow each iteration to query against it. SQL chose a simpler model. The restrictions on aggregation, subqueries, and mutual recursion all follow from this choice.

Delightql inherits these limitations because it transpiles to SQL and the semantics remain bound by the target. Where SQL's recursive CTEs fall short -- self-similar tree construction, recursive aggregation, shortest-path computation -- delightql falls short as well.
