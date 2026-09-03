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

```delightql
add:(x) :- x + 1        // arity 1
add:(x, y) :- x + y     // arity 2


employee(id, name) :- ... // arity 2
employee(*)  // arity determined by body
```

All definitions -- functions, sigma predicates, higher-order rulesviews, tables, facts -- must have the same fixed arity.

The following are forbidden:

```delightql
add:(x) :- x + 1        // arity 1
add:(x, y) :- x + y     // arity 2

employee(id, name) :- ...
employee(id,name,age)  :- ...
```

If you can write `foo(*)`, all `foo` definitions must agree on
arity. With argumentative heads, agreement is stricter: same arity AND same
names at each position.

This rule is stricter than most languages where a name can be overloaded by arity.
Delightql insists that there can only be one named entity per namespace and that
that entity always have the same arity.  The only place where this rule is relaxed
is common expressions which can shadow tables, views, and functions.
