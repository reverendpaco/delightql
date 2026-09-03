# Head Semantics {.dqlh}

The head of a rule names the entity and declares what it exposes.
How the head is written determines the output schema and how
multi-clause entities combine.

## Two Head Forms {.dqlh}

### Glob head {.dqlh}

```delightql
young(*)
  :- people(*), age > 20
```

The glob `*` passes through whatever the body produces. The entity
inherits its schema from the body. This is the permissive form --
the head makes no claim about column names or count.

### Argumentative head {.dqlh}

```delightql
young(name, age)
  :- people(*), age > 20
```

The head declares the entity's output schema: exactly `name` and
`age`, in that order. Every clause must satisfy this contract -- the
body must produce columns with those names (among possibly others).

The body may be wider than the head. The head *projects* from the body
by name, so wide source tables need not be narrowed in the body
before the head can name the subset. If the body does not produce a
column named in the head, then it is an error.

## Ground Terms in the Head {.dqlh}

A head position can hold a ground term (a literal) instead of a free
variable (a column name).

```delightql
bracket("old", last_name, first_name)
  :- people(*), age > 40
```

Ground terms in the head inject constants into the output and
provide choice semantics for multi-clause disjunctive rules.

## Multi-Clause Rules {.dqlh}

```delightql
bracket("old", last_name, first_name)
  :- people(*), age > 40
bracket("toddler", last_name, first_name)
  :- people(*), age < 4
bracket(category, last_name, first_name)
  :- people(*)
```

Multi-headed rules with the exact same named and shaped head are
called disjunctive rules.  The *disjunctive* in disjunctive rules
is a reference to the logical `OR` and manifests in different
ways depending on the context:

  - sum types in algebraic data types
  - tagged unions and/or variants in many programming languages
  - union in set theory (and therefore SQL)
  - choice in grammar rules
  - `OR` in logic

The meaning of multi-headed clauses is exactly **UNION ALL**.

### Disjunctive Rules: Consistency of Head Forms {.dqlh}

All clauses of the same entity must use the same head form -- either
all glob or all argumentative. Mixing is an error:

```delightql
-- OK: all glob
data(*) :- source_a(*)
data(*) :- source_b(*)

-- OK: all argumentative
data(x, y) :- source_a(*)
data(x, y) :- source_b(*)

-- ERROR: mixed head forms
data(*) :- source_a(*)
data(x, y) :- source_b(*)
```

## Disjunctive Rules: Union Semantics by head form {.dqlh}

Glob heads and argumentative heads use the exact same union strategy.
Both syntaxes must publish exactly the same heading: identical column names in identical order.
The programmer must explicitly reconcile disagreement across multiple bodies.


### Column naming with ground terms {.dqlh}

Argumentative heads require strict agreement: every clause must have
the same number of positions, and **free variables at each position
must use the same name across all clauses**. If clause 1 has
`(name , age)` and clause 2 has `(age , name)`  then this is
an error.

Ground terms (constants) are nameless -- they do not contribute a
column name for a position. Free variables provide names. The rules:

- If a position has free variables in one or more clauses, all
  those free variables must use the **same name**. Constants in other
  clauses are compatible (they provide a value but no name).
- Conflicting names at the same position is an error.

```delightql
-- OK: "category" names position 1; constants in other clauses are compatible
bracket("old", last_name, first_name)
  :- people(*), age > 40
bracket("toddler", last_name, first_name)
  :- people(*), age < 4
bracket(category, last_name, first_name)
  :- people(*)

-- ERROR: position 1 named "motto" in one clause, "city" in another
bracket("old", last_name, first_name)
  :- people(*), age > 40
bracket(motto, last_name, first_name)
  :- people(*)
bracket(city, last_name, first_name)
  :- people(*)
```
