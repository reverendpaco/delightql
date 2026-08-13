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
ancestor(person, anc) :-
  parent(person, anc)
ancestor(person, anc) :-
  parent(person, p), ancestor(p, anc)
```

**Recursive CTEs** are defined inline in query mode, scoped to a single query:

```delightql
ancestor(*) : parent(*) |> (person, parent as anc)
ancestor(*) : parent(*) as p, ancestor(*) as a, p.parent = a.person
    |> (p.person, a.anc)
ancestor(*)
```

Both forms transpile to `WITH RECURSIVE`. The choice depends on whether the recursive logic is reusable (rule) or ad-hoc (CTE).

## The Anatomy of Recursion {.dqlh}

Every recursive definition has two components:

**Base clauses** provide initial rows without self-reference. These are SQL's "anchor members":

```delightql
_(n @ 1) : counter                    // literal base case
org(*), title = "CEO" : mgmt          // filtered base case
edge(*) |> (origin, dest) : reachable // projected base case
```

**Recursive clauses** reference the predicate or CTE being defined. These are SQL's "recursive members":

```delightql
counter(*), n < 100 |> (n + 1 as n) : counter
mgmt(*) as m, org(*) as o, o.boss = m.name : mgmt
reachable(*) as r, edge(*) as e, r.dest = e.origin
    |> (r.origin, e.dest) : reachable
```


Base clauses must precede recursive clauses in source order: names
resolve strictly left to right, and a recursive clause reads a name the
base clause creates. Writing the recursive clause first is refused
("Table not found").

## Evaluation Model {.dqlh}

SQL's recursive CTEs evaluate using a **working table** algorithm:

1. Execute all base clauses; their results form the initial working table
2. Execute the recursive clause with the working table as input
3. The output becomes the new working table
4. Repeat until the working table is empty
5. Return the union of all iterations

This is **bottom-up** or **co-recursive** evaluation: starting from known facts, derive new facts, repeat until fixed point. It resembles dynamic programming more than classical recursion.

The critical implication: **the recursive clause sees only the previous iteration's rows, not the full accumulated result**. This is why certain operations are prohibited -- they would require access to rows that haven't been computed yet or have already been consumed.


## What Recursion Can Express {.dqlh}

SQL's recursive model handles a well-defined class of problems:

**Hierarchical traversal** -- org charts, bill of materials, folder structures:

```delightql
folders(*), parent_id = null |> (id, name, 0 as depth) : tree
folders(*) as f, tree(*) as t, f.parent_id = t.id
    |> (f.id, f.name, t.depth + 1) : tree
tree(*)
```

**Transitive closure** -- reachability, ancestry, dependency graphs:

```delightql
edge(*) |> (origin, dest) : reachable
reachable(*) as r, edge(*) as e, r.dest = e.origin
    |> (r.origin, e.dest) : reachable
reachable(*) |> %(*)  // deduplicate
```

**Sequence generation** -- numeric ranges, date series, iteration:

```delightql
_(d @ date:(2024, 1, 1)) : dates
dates(*), d < date:(2024, 12, 31)
    |> (d + interval:(1, 'day') as d) : dates
dates(*)
```

**Iterative computation** -- any algorithm expressible as "given previous state, compute next state":

```delightql
_(iter @ 0, x @ 1.0, target @ 2.0) : newton
newton(*), abs:(x*x - target) > 0.0001, iter < 100
    |> (iter + 1, (x + target/x) / 2.0 as x, target) : newton
newton(*) |> %(target ~> max:(x) as sqrt)
```

## What Recursion Cannot Express {.dqlh}

The working-table model imposes fundamental limitations. These are not
arbitrary restrictions -- they follow from the evaluation semantics.

### No Aggregation in Recursive Clauses {.dqlh}

Aggregation requires access to multiple rows. The recursive clause sees only the working table (previous iteration), not the full accumulated result.

```{.delightql .bad}
// INVALID -- cannot aggregate within recursion
subtree(*) as s, node(*) as n, n.parent = s.id
    |> (n.id, s.total + n.value as total) : subtree  // seems ok?

// But this fails:
subtree(*) as s, node(*) as n, n.parent = s.id
    ~> (s.id, sum:(n.value) as total) : subtree  // aggregation -- NOT ALLOWED
```


### No Subqueries Referencing the Recursive Target {.dqlh}

A subquery inside the recursive clause cannot reference the CTE being defined:

```{.delightql .bad}
// INVALID -- subquery references 'paths'
edge(*) as e, paths(*) as p, e.origin = p.dest,
    \+ paths(*, e.dest = dest)  // "dest not already reached" -- NOT ALLOWED
    |> (p.origin, e.dest) : paths
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


### Single Self-Reference {.dqlh}

The recursive clause may reference the target exactly once:

```{.delightql .bad}
// INVALID -- two self-references
paths(*) as p1, paths(*) as p2, p1.dest = p2.origin
    |> (p1.origin, p2.dest) : paths
```

This would require joining the working table against itself, which SQL doesn't support in recursive CTEs.


## Termination {.dqlh}

Recursive CTEs terminate when the recursive clause produces no new rows. This happens when:

- A `WHERE` condition filters out all candidates
- A join finds no matches
- The depth limit (`#`) is reached
- The data is exhausted (finite traversal)

**Ensuring termination:**

For sequence generation, always include a bound:

```delightql
nums(*), n < 1000 |> (n + 1 as n) : nums  // terminates at 1000
```

For graph traversal over potentially cyclic data, track visited nodes:

```delightql
edge(*) |> (origin, dest, ',' || origin || ',' as visited) : paths
edge(*) as e, paths(*) as p, p.dest = e.origin,
    p.visited not like '%,' || e.dest || ',%'
    |> (p.origin, e.dest, p.visited || e.dest || ',') : paths
```

For unknown depth, use `#` as a safety limit:

```delightql
tree(*) as t, node(*) as n, n.parent = t.id, # < 100
    |> (...) : tree
```


## UNION vs UNION ALL {.dqlh}

By default, delightql emits `UNION ALL` -- duplicates across iterations are preserved. This is efficient and correct for most traversals.

For graph traversal where the same node may be reached via multiple paths, duplicates accumulate. To deduplicate the final result:

```delightql
edge(*) |> (origin, dest) : reachable
edge(*) as e, reachable(*) as r, r.dest = e.origin
    |> (r.origin, e.dest) : reachable
reachable(*) |> %(*)  // deduplicate at the end
```


## Higher-Order Recursive Predicates {.dqlh}

Recursive rules can be parameterized, deferring the base case:

```{.delightql .am}
reports_to(boss)(name) :- employee(*), name = boss.
reports_to(boss)(name) :-
    employee(*) as e,
    reports_to(boss)(*) as r,
    e.manager = r.name
    |> (e.name).
```

Each invocation monomorphizes to a concrete `WITH RECURSIVE`:

```delightql
reports_to("Alice")(*)  // who reports to Alice?
reports_to("Bob")(*)    // who reports to Bob?
```

The higher-order parameter `boss` is inlined into the anchor clause at query time. The recursive structure itself doesn't change -- only the starting point.

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

## Delightql Recursive Apology {.dqlh}


A true fixed-point engine -- like those in Datalog systems -- would maintain the full set of derived facts and allow each iteration to query against it. SQL chose a simpler model. The restrictions on aggregation, subqueries, and mutual recursion all follow from this choice.

Delightql inherits these limitations because it transpiles to SQL and the semantics remain bound by the target. Where SQL's recursive CTEs fall short -- self-similar tree construction, recursive aggregation, shortest-path computation -- delightql falls short as well.
