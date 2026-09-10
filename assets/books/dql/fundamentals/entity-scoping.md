
# Entity Shadowing and Uniqueness {.dqlh}

There are three axes for understanding the identity of an entity:

  - **name**:  the string identifier of the table, view, function or rule
  - **arity**:  the number of columns
  - **kind**: whether the entity was defined as rule-first, function-first, sigma-condition-first or mounted from a source system

Delightql chooses a conservative identification scheme whereby
**name alone uniquely identifies an entity**.  This scheme prohibits
the following in a single namespace:

 - two entities with the same name but different arities
 - two entities with the same name but different types (function vs rule vs sigma rule)

This does **not** prohibit the definition of shadowing entities, which may have
different arities and/or kinds.  Shadowing occurs when an entity is defined in
a context/scope with higher precedence.

Some entities are always uniquely identified by a fully-qualified name -- the concatentation of
the namespace with a name.  Referencing that entity with its fully-qualified name is guaranteed
to access the entity in a way that will never be ambiguous. CTEs and aliases don't exist in
a namespace and are always referenced without qualification.

Ambiguity can only ever occur when namespaces are stripped from entities that
have a namespace during *enlistment*. Enlistment is an ergonomic choice by a
programmer that comes with the danger of having two entities introduced with
the same name.

It is up to the programmer to remove that ambiguity via qualification, aliasing,
or renaming.

![Entity Shadowing](images/entity-shadowing.svg)


## Contexts {.dqlh}

Contexts are the scopes into which an entity may be defined.

Entities can exist (be defined) in the following contexts:

 - inside a data namespace (made visible by a `mount!`)
 - inside a lib namespace (made visible by a `consult!`)
 - inside a shadow namespace (created by a `temp_table!`, `temp_view!`)
 - inside **the** primary context ( `home` for REPL usage, and the primary namespace for file-based consulting)
 - as a CTE
 - as a nominalized alias
 - within the target scope


## Context Shadowing {.dqlh}

We will use the notation `T[c]` to discuss the partial
order that rules shadowing.  It should be read aloud as "the entity `T`
defined in context `c`" -- for example `users[cte]` means "the entity
with the name `users` defined as a CTE".

The context shadowing rule states that the following order always applies:

```
T[alias]
  >  T[cte]
  >  T[primary-context]
  > { all other contexts }
```


Where the `>` indicates that the scope to the left supersedes
and redefines all entity references to the right.

The "all other contexts" refers to the combination of
separately enlisted namespaces, none of which
has priority over another -- which we denote with the `||`
parallel symbol.


```
{ all other contexts } =
  { T[shadow-NS1] > T[NS1] }
  ||
  { T[shadow-NS2] > T[NS2] }
  ||
  { T[NS3] } }
```

As an example:

```{.delightql .numberLines}
mount!("some.db","ns::foos")(*)
enlist!("ns::foos")(*)

foo(*) , fizzbuz="NA" |> temp_table!(foo(*))(*)

(~~ddl foo(*) :- _(a@1;2;3) ~~)

bar(*), baz<20 : foo
fizz(*) as foo |> (foo.buzz)
```

In the example above we assume the existence of a `foo` entity
within the namespace `ns:foos`.

 - after line 2 an enlisted `foo` is available (from `ns:foos`)
 - after line 4 the temporary table `foo` shadows the original.  It is auto-enlisted from the namespace `sys::shadow::ns::foos`.
 - after line 6 the primary context definition of `foo` shadows the temporary table
 - after line 8 where the CTE `foo` is defined, the query's use of `foo` will shadow all others
 - at line 9 an alias names another entity `as foo` which shadows all others


> Warning:  an alias only lasts until the next scope barrier.  Thus,
> in
>
> ```delightql
> bar(*), baz<20 : foo
> fizz(*) as foo |> (foo.buzz), foo(*)
> ```
>
> The `foo(**)` at the end of the query refers to the CTE and not the alias, because
> the `|> (foo.buzz)` has created a scope barrier that has removed the aliased `foo` from
> scope.

## Materialization Shadowing {.dqlh}

Materialization shadowing is
exactly the SQL DDL semantic available
via `CREATE TEMPORARY TABLE` and `CREATE TEMPORARY VIEW`.
It is the mechanism by which a session scoped entity
may exist only for the duration of that session.

The order induced by the materialized shadow is:

```
T[shadow-NS] > T[NS]
```

The delightql directives  `temp_table!` and `temp_view!`
may be used to create these shadowing entities.

```delightql
mount!("some.db","data::baz")(*)
enlist!("data::baz")(*)

foo(*) , fizzbuz="NA" |> temp_table!(foo(*))(*)
```

The above code will automatically place `foo` (the materialized shadow) within a special delightql namespace
under the `sys` sub-tree -- specifically `sys::shadow::data::baz`.  Therafter, delightql applies a rule
for any auto-enlisted references to first check the shadow namepace for resolution.

```delightql
foo(*) // resolves from sys::shadow::data::baz.foo
```


## Auto-enlist {.dqlh}

The following namespaces are auto-enlisted into any query:

  - main
  - std::shadow::main
  - std::prelude
  - std::predicates
  - std::meta

Additionally, the `home` namespace is auto-enlisted when
a user enters the REPL.

All entities from these auto-enlisted namespaces may be referenced without
namespace qualification in the current context.


## Amibguity in non-shadowing contexts

Any context that is not either a CTE, alias, or primary context
may be ambigous with other such contexts.  This is inclusive
of materialized shadows which still attach to their original
namespace.

```
  { T[shadow-NS1] > T[NS1] }
  ||
  { T[shadow-NS2] > T[NS2] }
  ||
  { T[NS3] } }
```

Multiple enlisted namespaces may export the same entity name. This overlap does
not itself cause an error. A bare reference is ambiguous only when more than
one distinct entity remains after applying the shadowing rules. Fully qualified
references remain available.  The bare references that are
shared from more than one enlisted namespaces and which have
no shadows are **ambiguous entitities**.

The usage of an **ambiguous entity** is an error and
must be resolved by the programmer.

## Binding follows lexical definition {.dqlh}

In the following example, the binding of
`items` within the body of `report` is a reference
to the un-qualified entity from `lib::a`.

```delightql
enlist!("lib::a")(*)

(~~ddl
report(*) :- items(*)
~~)

delist!("lib::a")(*)
```


After a `delist!`, we retain the reference
to `items` from its namespace because **it was defined lexically**.

The other option, dynamic scoping, where `items`  is
dynamically swapped in and out is **not** correct semantics.

The dependency retains its namespace identity, not a historical copy of its
implementation. Thus, a successful `reconsult!` of `lib::a` changes what
subsequent statements observe through that dependency.
