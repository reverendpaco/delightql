
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
to access the entity in a way that will never be ambiguous. CTEs don't exist in
a namespace and are always referenced without qualification. A relational alias
is not an entity at all: it names one occurrence (see
[Aliases name occurrences](#aliases-name-occurrences)).

Ambiguity can occur only at a tier that contains peer candidates, principally
the enlisted-namespace tier. Enlistment is an ergonomic choice that may make
two distinct entities available under one bare name. It is legal to construct
that state; the ambiguous name refuses only when mentioned.

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
 - within the target scope

A deep namespace path creates its missing prefixes as structural nodes.
For example, consulting into `a::b::c` creates `a` and `a::b` if needed,
but does not give them `lib` or `data` backing or entities. A later explicit
`consult!` may claim an unbacked prefix as a library, or `mount!` may claim
it as data. Either claim preserves existing children and refuses collisions
or replacement of an already backed namespace. Backing belongs to
each node independently: an empty library is still a library, and a child
does not inherit its parent's kind. Enlisting a parent does not make its
children's entities bare. This structural-prefix rule is ratified for
alpha; implementation is pending.

Removing the last child does not silently remove an implicit parent.
The empty structural node keeps its identity and can be claimed later.
An explicit `unconsult!` of its exact path removes that node alone if it
has no children or published dependents; it does not prune its parent.


## Context Shadowing {.dqlh}

We will use the notation `T[c]` to discuss the partial
order that rules shadowing.  It should be read aloud as "the entity `T`
defined in context `c`" -- for example `users[cte]` means "the entity
with the name `users` defined as a CTE".

The context shadowing rule states that the following order always applies:

```text
T[cte]
  >  T[primary-context]
  > { all other contexts }
```


Where the `>` indicates that the scope to the left supersedes
and redefines all entity references to the right.

The "all other contexts" refers to the combination of
separately enlisted namespaces, none of which
has priority over another -- which we denote with the `||`
parallel symbol.

The first nonempty tier wins. A same-named entity in a lower tier is not a
fallback if the selected entity has the wrong kind, arity, or heading.


```text
{ all other contexts } =
  { T[shadow-NS1] > T[NS1] }
  ||
  { T[shadow-NS2] > T[NS2] }
  ||
  { T[NS3] }
```

As an example:

```{.delightql .numberLines}
mount!("etl.sqlite","etl")(*)
enlist!("etl")(*)

partner_sale_2025_07_31(*), quantity > 0 |> temp_table!(etl.partner_sale_2025_07_31(*))(*)

(~~ddl partner_sale_2025_07_31(*) :- _(sale_id @ 4; 5; 6) ~~)

invoice(*), total > 20 : partner_sale_2025_07_31
media_type(*) as partner_sale_2025_07_31 |> (partner_sale_2025_07_31.name)
```

In the example above we assume the existence of a `partner_sale_2025_07_31` entity
within the namespace `etl`.

 - after line 2 an enlisted `partner_sale_2025_07_31` is available (from `etl`)
 - after line 4 the temporary table `partner_sale_2025_07_31`, created for `etl`, shadows
   the original within the `etl` candidate. The mounted database shares
   the primary connection's temp schema, so its exact identity is
   `sys::shadow::main.partner_sale_2025_07_31`; no extra enlist edge is created, and a bare
   `temp_table!(partner_sale_2025_07_31(*))(*)` would instead have created a session object for
   `main`, the default write target.
 - after line 6 the primary context definition of `partner_sale_2025_07_31` shadows the temporary table
 - after line 8 where the CTE `partner_sale_2025_07_31` is defined, the query's use of `partner_sale_2025_07_31` will shadow all others
 - at line 9 the alias `as partner_sale_2025_07_31` names the `media_type` occurrence, so `partner_sale_2025_07_31.name`
   addresses that occurrence's column. It does not rebind the name `partner_sale_2025_07_31`:
   a `partner_sale_2025_07_31(*)` read still selects the CTE.


> Warning: an alias names an occurrence, not an entity. In
>
> ```delightql
> invoice(*), total > 20 : partner_sale_2025_07_31
> media_type(*) as partner_sale_2025_07_31 |> (partner_sale_2025_07_31.name), partner_sale_2025_07_31(*)
> ```
>
> the `partner_sale_2025_07_31(*)` at the end of the query refers to the CTE. The alias never
> answers a relation name, and its column addressing ended at the scope
> barrier `|> (partner_sale_2025_07_31.name)`.

## Aliases name occurrences {.dqlh}

An alias (`as a`) names one read of a relation for **column addressing**. It
creates no relation that a later mention can call:

```delightql
employee(*) as a, employee(*) as b    // two fresh reads of Employee: a self-join
invoice(*) as a, a.total > 20         // a.Total addresses the first occurrence
album(*) as artist, artist(*) as c    // Artist(*) selects by the shadowing order above
```

While the occurrence is live, a qualifier addresses its columns. Qualifiers
address occurrences, never entities, so an entity that happens to be spelled
`artist` does not compete with the alias. In relation position, `artist(*)` is an
ordinary mention. It selects an independently defined `artist` by the shadowing
order, or it fails; it never reopens the occurrence. A read whose scope would
share the live name `artist` refuses, because two live scopes cannot share a name.

The same holds for a read of a CTE, of a mounted table, or of an aliased table
expression (`R as u(a, b)`). A pipe ends the occurrence's addressing; to
address a result further, name it (`|> (…) as v`).

## Materialization Shadowing {.dqlh}

Materialization shadowing is
exactly the SQL DDL semantic available
via `CREATE TEMPORARY TABLE` and `CREATE TEMPORARY VIEW`.
It is the mechanism by which a session scoped entity
may exist only for the duration of that session.

The order induced by the materialized shadow, inside a data namespace `D`, is:

```text
T[session object recorded as overlaying D] > T[D]
```

The delightql directives  `temp_table!` and `temp_view!`
may be used to create these shadowing entities.

```delightql
mount!("etl.sqlite","data::baz")(*)
enlist!("data::baz")(*)

partner_sale_2025_07_31(*) , quantity > 0 |> temp_table!(data::baz.partner_sale_2025_07_31(*))(*)
```

The target names `data::baz` as the durable namespace this session object
overlays. On SQLite, the mounted database shares the primary connection's
temp schema, so the object's exact shadow path is `sys::shadow::main.partner_sale_2025_07_31`,
not `sys::shadow::data::baz.partner_sale_2025_07_31`. Its recorded durable owner is still
`data::baz`. Bare selection applies that overlay to the `data::baz`
candidate before comparing it with peer enlisted namespaces; it does not
apply it to `main` merely because `main` appears in the shadow path. A second
durable owner on the same connection cannot create another physical temp
`partner_sale_2025_07_31` at that path; the second creation refuses and names the holder.

```delightql
mount!("etl.sqlite","data::baz")(*)
enlist!("data::baz")(*)
partner_sale_2025_07_31(*) , quantity > 0 |> temp_table!(data::baz.partner_sale_2025_07_31(*))(*)

partner_sale_2025_07_31(*)                   // resolves from the local shadow
data::baz.partner_sale_2025_07_31(*)         // exact durable entity
sys::shadow::main.partner_sale_2025_07_31(*) // exact session entity on this connection
```

The creation receipt reports both names: `target = data::baz.partner_sale_2025_07_31` is the
resolved data-namespace target, while `created = sys::shadow::main.partner_sale_2025_07_31` is
the exact session entity. For a durable `table!`, `target` and `created`
coincide. A bare creation target uses the session's default data-write
target (`main`); a qualified creation target must be data-backed. A source
that cannot run on that target's connection is refused, not relocated.
Without a database file, `main` is backed by the session's in-memory primary:
ordinary `table!` objects last for that database session, and temporary
objects use its temp schema.


## Addressing and enlistment {.dqlh}

Qualification selects an exact route:

```delightql
(~~ddl:"reports" daily(*) :- invoice(*) |> %(invoice_date ~> sum:(total) as total) ~~)

home::reports.daily(*)     // exact path
.::reports.daily(*)        // relative to the primary definition context
alias!("home::reports", "r")(*)
r.daily(*)                 // exact alias route
```

An unmarked qualifier is an exact top-level namespace or an explicit
`alias!` shorthand. It does not search the children of enlisted namespaces.
Enlisting a namespace makes that namespace's own entities bare; it does not
make child namespaces implicitly addressable and is not transitive.

The session begins with these four enlist edges:

  - `main`
  - `std::prelude`
  - `std::predicates`
  - `sys::meta`

`home` is different: it is the top-level primary definition context, not a
fifth auto-enlist. That is why its entities are bare and why they outrank
enlisted entities. `sys::ns.enlisted_namespace(*)` reports enlist edges only;
it does not report `home` merely for being the primary context.

Namespace aliases are qualifier shorthands. They do not grant bare entity
names and, like relational aliases, occupy no tier of the shadowing order.


## Ambiguity in non-shadowing contexts

Any context that is not a CTE or the primary context
may be ambiguous with other such contexts.  This is inclusive
of materialized shadows which still attach to their original
namespace.

```text
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
no shadows are **ambiguous entities**.

Using an **ambiguous entity** is an error and must be resolved by the
programmer. Enlistment itself still succeeds. Its receipt may list names that
have become ambiguous, but the list is advice, not a second resolution
authority. The diagnostic at the mention lists the complete set of distinct
candidate identities; enlist order and `main` do not break the tie.

Shadowing is by name before shape. A temporary `foo` may have a different
kind, arity, or heading from the durable `foo` it shadows. If the selected
temporary entity cannot serve a use, that use refuses; DelightQL does not
silently fall through to the durable entity.

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

`unconsult!` differs from `reconsult!`: it refuses if removing `lib::a`
would leave a published definition such as `report` depending on it. The
refusal names the blocking definition and namespace. A fully qualified
reference may have been written before its target was consulted; once that
target exists and the reference resolves, it too blocks removal. Change or
remove the dependent definition first, or rebuild the scripted session.
This alpha removal rule is ratified; implementation is pending.

When no published definition blocks removal, `unconsult!` also removes
the prompt's enlistments and namespace aliases pointing to the target.
Its receipt names these removed edges. A scratch rule created with
`(~~ddl … ~~)` is a definition, not one of those disposable prompt edges.

For an ordinary mounted data namespace, `unmount!` follows the same rule:
a published definition reading its tables blocks removal, while a successful
removal cleans prompt-owned enlistments and aliases and names them in the
receipt. The special lifecycle of `main` remains a separate question.

## Retracting a session definition {.dqlh}

`retract!` removes one complete session-authored entity family:

```delightql
(~~ddl foo(*) :- _(x @ 1) ~~)
retract!(foo(*))(*)
```

The argument is resolved by the ordinary rules. At the prompt, bare `foo`
therefore selects `home.foo` before an enlisted `foo`; the explicit spelling
is `retract!(home.foo(*))(*)`. This is family removal, not Prolog's
one-matching-clause retraction.

Only mutable definitions authored in `home` or another scratch namespace may
be retracted. Data tables, system entities, consulted-source definitions, and
CTEs refuse; an alias is never selected, so it is never a target. A dependent
definition or grounding also blocks removal. The receipt names the identity
removed; a later bare mention is resolved afresh and may therefore reveal a
lower-tier candidate.
