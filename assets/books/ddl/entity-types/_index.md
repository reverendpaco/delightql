# Rules {.dqlh}

The syntax of rules obey the general form:

```
  <HEAD>  <NECK>  <BODY>
```

In delightql these rules may be used for any of the following:

- Views
- Tables
- Higher-order views
- ER-contexts
- Sigma predicates
- Functions

All of these use the `:-`{.delightql .sigil} neck. The first five define
relations; functions define the special subset of relations that are functions.


## Arity and Naming {.dqlh}



A rule's **arity** is the number of arguments in its head.

Definitions invoked with explicit arguments -- functions, sigma predicates,
higher-order rules -- may share a name with different arities. The call site
disambiguates:

```delightql
add:(x) :- x + 1
add:(x, y) :- x + y

// Invocation is unambiguous
add:(5)      // add/1
add:(5, 3)   // add/2
```

Definitions queried with the glob -- views, tables, facts -- must have fixed arity. The glob presumes a single schema:

```delightql
employee(id, name) :- ...
employee(*)  // expects one schema
```

If you can write `foo(*)`, all `foo` definitions must agree on
arity. With argumentative heads, agreement is stricter: same arity AND same
names at each position. See [Head Semantics](#head-semantics) for the full
rules.
