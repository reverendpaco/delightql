# Namespaces {.dqlh}


Namespaces organize definitions and data. They provide isolation,
qualification, and a mechanism for code reuse across databases.

## What Namespaces Are {.dqlh}

A namespace is a container for entities -- tables, views, rules, functions.
Every entity lives in exactly one namespace.

Namespaces are hierarchical, separated by `::`:

```delightql
data::production
lib::analytics
scripts::etl
```

Entities within a namespace are accessed with  the period `.`{.delightql .sigil}:

```delightql
data::production.users(*)
lib::analytics.clean_name:(text)
scripts::etl.daily_load!(*)
```

The `::` separates namespace parts. The `.` separates namespace from entity.

## Namespace Types {.dqlh}

Namespaces fall into four categories based on what they contain and how they're used.

### Pure Rules Namespaces {.dqlh}

Contain functions, sigma predicates, transpilation rules, and higher-order
views with no external references. Portable -- they don't depend on any
database -- but may depend on other pure namespaces.

```delightql
// In lib::string
clean_name:(text) :- text /-> trim:() /-> upper:()
format_email:(name, domain) :- name ++ "@" ++ domain
```

Pure namespaces can be used anywhere. They have no data dependencies to resolve.

### Derived Rules Namespaces {.dqlh}

Contain rules that reference external tables. These namespaces come in two forms:

**Groundable**  --  has free variables (unqualified table references):

```delightql
// In lib::analytics (groundable)
young_users(*) :- users(*), age < 30   // 'users' is a free variable
```

The reference to `users` must be resolved before use. See [Grounding].

**Pre-grounded**  --  all references are qualified:
```delightql
// In lib::analytics (pre-grounded)
young_users(*) :- data::production.users(*), age < 30
```

No free variables. Ready to use immediately, but tied to a specific data namespace.

### Data Namespaces {.dqlh}

Map to physical database connections. Contain tables and views introspected from the database.
```delightql
mount!("sales.db", "data::sales")
// Now: data::sales.orders(*), data::sales.customers(*)
```

Data namespaces are the ground truth -- they hold actual data.

When a qualified mount names missing ancestors, `mount!()` creates those path
segments as structural `container` namespaces. For example, mounting
`data::sales` creates a queryable `data::(*)` parent and a `data::sales` data
leaf. The parent has no database source of its own; mount lifecycle operations
apply to the leaf.

## Namespace Classification {.dqlh}

A namespace's type is determined by its contents, not its path:

| If it contains...                               | It's classified as... |
|-------------------------------------------------|-----------------------|
| Only functions, sigma predicates, pure HO-views | Pure                  |
| Any rule referencing external tables            | Derived               |

: Namespace classification by contents


Data namespaces are separate -- they're created by `mount!()` and contain database tables, not rules. They're the target of grounding, not the subject.

## Conventional Prefixes {.dqlh}

By convention, namespace paths indicate their type:

| Prefix | Intended for | Typically created by |
|--------|--------------|----------------------|
| `data::` | Database connections | `mount!()` |
| `lib::` | Pure and derived rules | `consult!()` |
| `main` | Default working namespace | Implicit |

: Conventional namespace path prefixes

These are conventions only, not constraints. The system determines namespace type by
analyzing contents, independent of path.

## Built-in Namespaces {.dqlh}

Several namespaces exist automatically.

## main {.dqlh}

The default working namespace for the REPL. When you use the REPL
interactively, you're operating in `main`:

```delightql
active_users(*) :- users(*), status = "active"
// Equivalent to: main.active_users(*)
```

When you `enlist!()` a namespace in the REPL, you're making its entities
available in `main` without qualification.

More generally, every execution context has a working namespace. In the REPL,
it's `main`. During `consult!("file.dql", "lib::foo")`, the working context is
`lib::foo`--definitions in that file go into `lib::foo`. During
`run!("file.dql")`, the working context inherits from the caller.

### lib::std::prelude {.dqlh}

Core pseudo-predicates, universally available. This is a partial list -- see [Standard Library Reference] for the complete set.

| Pseudo-predicate | Purpose |
|------------------|---------|
| `mount!()` | Load database connection |
| `consult!()` | Load DQL rules file |
| `enlist!()` | Enable unqualified access |
| `delist!()` | Remove enlisted namespace |
| `run!()` | Execute query file |

: Core pseudo-predicates in lib::std::prelude

The DML directives (`insert!()`, `update!()`, `delete!()`) are covered in [DML]; multi-step effect programs are specified in `SEMANTICS/effect-algebra-law.md` (repository root).

No explicit enlist needed -- these are available everywhere.

[The pseudo-predicates that load and inspect namespaces are themselves defined in a namespace. This circularity is intentional -- the system is self-describing. You can query `sys::entities.entity(*)` to see all built-in entities, including these pseudo-predicates.]{.sidenote}

### lib::std::predicates {.dqlh}

Built-in sigma predicates, universally available:
```delightql
users(*), +like(name, "A%"), +between(age, 18, 65)
```

See [Standard Library Reference] for the complete list.

### sys::* (Introspection) {.dqlh}

Metadata namespaces for system introspection:

| Namespace | Contains |
|-----------|----------|
| `sys::ns` | Namespaces, enlisted relationships, activated entities |
| `sys::entities` | Entities, types, references, resolutions |
| `sys::cartridges` | Cartridges, source types, connections |
| `sys::execution` | Compilations, recursion stack (diagnostics) |

: System introspection namespaces

Not auto-enlisted. Query explicitly when needed:
```delightql
sys::ns.namespace(*)
sys::entities.entity(*), type = 10   // database tables
sys::cartridges.cartridge(*)
```

## Pseudo-predicates and "attaching" context {.dqlh}

There are several functor forms ending with an exclamation
point that are used to bring rules, facts, and data
into scope and within a namespace.


### mount!()  --  Database Connections {.dqlh}

Opens a database and introspects its tables:
```delightql
mount!("sales.db", "data::sales")
```

Side effects:

1. Creates namespace `data::sales`
1. Introspects tables and views
1. Registers entities in namespace

After mounting, tables are accessible:
```delightql
data::sales.orders(*)
data::sales.customers(*)
```

### consult!()  --  DQL Rules {.dqlh}

Loads a `.dql` file containing rules:

```delightql
consult!("analytics.dql", "lib::analytics")
```

The file contains rule definitions:
```delightql
// analytics.dql
young_users(*) :- users(*), age < 30
high_value(*) :- orders(*), total > 1000
```

Side effects:

1. Creates namespace `lib::analytics`
2. Parses file
3. Creates session views/functions
4. Registers entities in namespace

Rules are now accessible (qualified or via enlist):
```delightql
lib::analytics.young_users(*)
```

### enlist!()  --  Unqualified Access {.dqlh}

Makes a namespace's entities available without qualification:
```delightql
enlist!("lib::analytics")

// Now can write:
young_users(*)
// Instead of:
lib::analytics.young_users(*)
```

Enlisting doesn't load anything -- the namespace must already exist.

### delist!()  --  Remove Enlisted Namespace {.dqlh}

Removes a namespace from enlisted scope:
```delightql
delist!("lib::analytics")

young_users(*)                      // Error: not found
lib::analytics.young_users(*)       // Still works (qualified)
```


## Grounding {.dqlh}

Groundable namespaces have free variables -- references to tables that aren't defined in the namespace. Grounding binds those variables to a data namespace.

### Formal Rule {.dqlh}

In the expression `F^S.e(*)`:

- Only entities of **S** are visible. The entity `e` must be defined in S.
- **F** is never directly accessible. It supplies bindings for free variables
  inside S's entity bodies.
- Grounding does **not** grant access to S's other entities (e.g.,
  functions defined in S are not made available in pipe expressions).
  Functions must be accessed via qualification (`S.func:(x)`) or
  `enlist!("S")`.

Put differently: `F^S.e(*)` means "from S, give me `e`, and when `e`'s body
references tables, find them in F." It does not mean "merge F and S together."

### The Problem Again {.dqlh}

```delightql
// In lib::analytics (groundable)
young_users(*) :- users(*), age < 30
```

`users` is referenced but not defined. This namespace can't be used until `users` is bound to an actual table.

### Query-Time Grounding {.dqlh}

Use `^` to ground at the point of use:
```delightql
data::production^lib::analytics.young_users(*)
```

This binds `users` to `data::production.users` for this query.

Query-time grounding uses **lazy validation** -- only the accessed entity and
its dependencies are checked. Other entities in the namespace may have
unresolved references; they won't cause failure unless you use them.

```delightql
// lib::analytics has:
//   young_users(*) :- users(*), age < 30       // OK; users exists in production
//   revenue_report(*) :- sales(*), amount > 0  // FAIL; sales doesn't exist

data::production^lib::analytics.young_users(*)    // OK
data::production^lib::analytics.revenue_report(*) // FAIL: sales not found
```



### Permanent Grounding {.dqlh}

Query-time and enlist-time grounding are temporary. For a permanent binding, use `ground!()`:
```delightql
ground!(data::production, lib::analytics, "lib::analytics_prod")
```

All three arguments are required. The first two are namespace paths; the
third is a string literal naming the new namespace.

This:

1. Validates **all** entities in `lib::analytics` against `data::production`
   (strict validation). If any entity has an unresolved table reference, the
   entire operation fails -- nothing is created.
2. Creates a new namespace `lib::analytics_prod`
3. Copies all entities with free variables bound to `data::production`
4. The new namespace is pre-grounded -- no `^` operator needed

The result is a new namespace, not a mutation of the original. This prevents
stateful bugs and makes the operation idempotent.

### Chained Grounding {.dqlh}

Ground through multiple layers:
```delightql
data::production^lib::base^lib::extended.final_view(*)
```

Each `^` binds the namespace to its right against the accumulated context to its left.

### Grounding as Inverse Enlist {.dqlh}

Another way to think about grounding: `enlist!()` brings a namespace's entities into your scope; grounding injects bindings into a namespace's scope.

```delightql
// Enlist: bring lib::analytics INTO main
enlist!("lib::analytics")

// Ground: inject data::production INTO lib::analytics
data::production^lib::analytics
```

Grounding reaches into the groundable namespace and says "when you reference `users`, you mean `data::production.users`."

### Validation Summary {.dqlh}

| Operation                    | Validation               | Persistence               |
|------------------------------|--------------------------|---------------------------|
| `data::ns^lib::ns.entity(*)` | Lazy (just this entity)  | Query only                |
| `enlist!(data::ns^lib::ns)`  | Strict (whole namespace) | Enlist scope              |
| `ground!(data, lib, "new")`  | Strict (whole namespace) | Permanent (new namespace) |

: Grounding validation summary

### Constraints {.dqlh}

**No intersection.** The ground namespace and groundable namespace cannot share entity names. If both define `users`, grounding is ambiguous and fails.

**Same database technology.** Cross-database grounding (e.g., SQLite namespace against PostgreSQL namespace) is not supported.

## Imprinting {.dqlh}

Session-scoped entities (created with `:-`) disappear when the session ends. Imprinting makes them permanent.

More so than most of what we've discussed before:  **this is where actual SQL DDL will be generated**.

### The Problem {.dqlh}
```delightql
consult!("schema.dql", "lib::schema")
// Creates session views

// Session ends... views are gone
```

### The Solution {.dqlh}
```delightql
imprint!("lib::schema", "data::production")
```

Imprinting:

1. Validates that all entities can resolve against the target (strict validation)
2. Generates DDL (`CREATE VIEW`, `CREATE TABLE`)
3. Executes DDL on the target database
4. Entities now exist permanently in the data namespace

### Grounding and Imprinting {.dqlh}


Grounding and imprinting are highly related: where
grounding proves compatibility, imprinting makes it permanent.

If `data::production^lib::analytics` is valid grounding, then `imprint!("lib::analytics", "data::production")` is valid imprinting. The grounding operation proves that the derived namespace can bind against the data namespace. Imprinting persists that binding.

```delightql
// 1. Load database
mount!("prod.db", "data::production")

// 2. Load groundable rules
consult!("analytics.dql", "lib::analytics")

// 3. Ground and test (lazy validation)
data::production^lib::analytics.young_users(*) |> count:(*)

// 4. Commit to grounding (strict validation)
enlist!(data::production^lib::analytics) as analytics

// 5. Work confidently
analytics.young_users(*)
analytics.revenue_report(*)

// 6. Persist (strict validation)
imprint!("lib::analytics", "data::production")
```

Steps 4 and 6 both perform strict validation. If enlist succeeds, imprint will succeed (assuming no concurrent changes).
