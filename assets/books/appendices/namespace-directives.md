# Namespace Directives {.dqlh}

## The image {.dqlh}

A DQL session is a filesystem. You mount databases, install libraries,
create directories. When you close the lid, the state persists. When
you reopen it, everything is where you left it.

```
~::                             -- your home directory
├── data::wh                    -- a mounted database
├── analytics                   -- a consulted DDL library
│   └── helpers                 -- the library's internal dependency
├── analytics::grounded         -- library bound to data
└── scratch                     -- a namespace you made
```

`~::` is home. `::` is root (where `sys` and `std` live). Directives
are the shell commands that shape this tree. Queries run inside it.

The image is a SQLite file -- the bootstrap database serialized to disk.
Not a replay script, but the actual state: namespace tree, entity
definitions, connection metadata, timestamps, history. Since DQL
already uses SQLite for its internal state, the image format is the
system's own storage format. Dogfooding.

```bash
# Ephemeral (default) -- fresh home, dies on exit
echo 'users(*)' | dql query --db warehouse.db

# Persistent -- your laptop
dql --session workspace.db --db warehouse.db -i
> mount!("ref.db", "data::ref")
> consult!("analytics.dql", "analytics")
> weekly_report(*)
> .quit                        # state saved to workspace.db

# Next day -- everything is where you left it
dql --session workspace.db -i
> weekly_report(*)             # just works
```

The image is queryable. `mount!("old_session.db", "prev")` and browse
what you had last week. Diff two environments by joining their
bootstrap tables. The session IS a database.

This is the Smalltalk image model applied to a query environment.
Smalltalk's images were opaque heap dumps. Jupyter notebooks improved
this with ordered cells, but introduced a desync problem -- run cells
out of order and the kernel diverges from what the notebook shows.
A DQL image has neither problem: it's inspectable (it's SQLite) and
it's the actual state (not a recipe that might diverge).

## Directives {.dqlh}

Queries transpile to SQL. Directives shape the environment in which
queries run. `mount!` doesn't produce SQL -- it connects a database.
`consult!` loads view definitions. `enlist!` makes names visible.

Every directive produces, consumes, borrows, or transforms a namespace.

### Produce {.dqlh}

```dql
mount!("warehouse.db", "data::wh")        -- connect database → DataNs
consult!("analytics.dql", "analytics")     -- load DDL file → LibNs
copy!("subset")                            -- pipe terminal: create from entity metadata → LibNs
consult_tree!("models/", "lib")            -- directory tree → nested LibNs
mount_tree!("postgres://host/db", "data")  -- database catalog → nested DataNs
```

The `_tree` variants mirror an external hierarchy (filesystem or database
catalog) into the namespace tree. The caller names the root; the source
names the branches. `models/util/greet.dql` becomes `lib::util::greet`.

### Consume {.dqlh}

```dql
unmount!("data::wh")
unconsult!("analytics")
imprint!("analytics", "data::wh")         -- materializes views as tables, consumes LibNs
```

`imprint!` is linear -- the library namespace is consumed. This prevents
ghost duality (abstract definitions alongside concrete tables that
inevitably drift).

### Borrow {.dqlh}

```dql
ground!("data::wh", "analytics", "analytics::g")   -- bind lib to data → GroundedNs
serialize!("analytics", "backup.dql")               -- write to file
```

### Transform {.dqlh}

```dql
refresh!("data::wh")           -- re-introspect schema
reconsult!("analytics")        -- reload from file
```

### Scope-local (visibility) {.dqlh}

```dql
enlist!("analytics")           -- bare names visible in my scope
alias!("data::wh", "wh")      -- wh.users(*) shorthand
delist!("analytics")           -- remove enlistment + alias
```

Scope-local operations are saved/restored at DDL boundaries. A DDL
that enlists a namespace doesn't pollute its caller.

### Scratch namespaces {.dqlh}

Inline DDL (`(~~ddl:"name" ~~)`) creates scratch namespaces that are
**ambient** -- they automatically bind to the database they were created
under. A consulted library needs explicit `ground!` to connect its
table references to data. A scratch namespace doesn't -- you're
defining views against the database that's right here, and the system
captures that binding at creation time.

```dql
(~~ddl:"helpers"
  young(*) :- users(*), age < 20
~~)
enlist!("helpers")
young(*)        -- users resolves against the current database
```

See `book/design/inline-ddl.md` for details on ambient binding,
provenance, and the relationship between scratch and consulted
namespaces.

### Execution {.dqlh}

```dql
play!("setup.dql")                  -- execute in my scope (source)
exec!("report.dql") |> (total)      -- execute, return last expression
run!("job.dql", "sandbox")          -- isolated sub-session
save!()                             -- persist ~:: to session file
```

## Pipe schemas {.dqlh}

Every directive produces one unnamed positional column: the namespace
it affected. No status column -- rows mean success, errors mean failure.

```dql
consult!("a.dql","ns1";"b.dql","ns2")(*) |> enlist!()
lib::(*) |> pick("view1";"view2") |> copy!("subset")
mount!("a.db","da";"b.db","db")(*) |> enlist!()
```

Scalar-lifted arguments (`;` between pairs) produce multiple rows.
Pipe terminals read the single column positionally.

## Nesting {.dqlh}

DDL files don't know their own name. The caller chooses:

```dql
consult!("analytics.dql", "analytics")   -- caller's choice
consult!("analytics.dql", "reports")     -- different caller, different name
```

A DDL that needs helpers cannot self-nest -- it doesn't have
`crate::` or `__name__`. Auto-nesting solves this: directives inside
a DDL are prefixed under the DDL's namespace automatically.

```dql
-- Inside analytics.dql:
consult!("helpers.dql", "helpers")     -- becomes analytics::helpers
consult!("shared.dql", "::shared")    -- :: escapes to global root
```

| Prefix | Target | Unix analogy |
|--------|--------|--------------|
| *(bare)* | relative to current DDL | `./` |
| `~::` | session root | `~/` |
| `::` | global root | `/` |

When two DDLs consult the same file, the namespace tree has two
entries. The engine shares resources behind the scenes (connections
are ref-counted by URI). The semantics are value-level copies; the
implementation shares structure. Functional data structures.

## Ownership {.dqlh}

Namespace directives have ownership semantics. Each directive either
produces, consumes, borrows, or transforms a namespace resource.

Key rules:
- Can't `unmount!` a DataNs that's borrowed by a `ground!`
- Can't `unconsult!` a LibNs that's borrowed by a `ground!`
- `imprint!` consumes the LibNs -- use-after-imprint is an error
- `delist!` drops both enlistments and aliases
- Destroying a parent namespace cascades to children

These enforce real invariants (no dangling views, no stale groundings)
through the type system rather than programmer discipline.

Full directive signatures with ownership annotations are in
`DESIGN-namespace-directives.md`.
