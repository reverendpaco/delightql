# The Two Necks {.dqlh}


The **neck** separates a rule's head from its body. Delightql has two necks, each
defining a different scope and extent:

| Neck | Name            |
|------|-----------------|
| `:`  | Shadow neck     |
| `:-` | Rule neck       |

: The two neck operators

## Extent vs Scope {.dqlh}

There is a difference between when and where a rule definition is available.
The terminology is known by the names *extent* and *scope*.

**Scope** refers to the *spatial* visibility of a definition.
It asks *where* an abstraction -- a name -- is available for use.

**Extent** is *temporal* lifetime.  It asks *when* an abstraction
is available for use.  In contrast to regular programming languages,
extent matters more for databases where tables and views outlive
a process.


The necks in delightql map to different types of scope and extent:

- Query-extent (`:`) -- Exists only for the duration of one query. These are CTEs.  These also have a scope limited by a query.
- Session-extent (`:-`) -- Exists for the duration of the connection.  These are temporary views, tables or inlinable definitions.  Their scope is
  determined by namespacing -- the subject of a later chapter.
- Permanent-extent -- Persists after disconnection. These are tables and views.


## Shadow Neck `:` {.delightql .sigil .dqlh}

The shadow neck defines a momentary definition with limited scope. The definition exists
only for the single query in which it appears:

```dql
young(x) : users(x), age < 30
young(*)
```


A shadow-neck definition may shadow an existing table or view of the same name
for the duration of the query. This scoping behavior is unique to the shadow
neck.

Shadow-neck definitions are not DDL -- they are reviewed here for syntactic
similarity to rules.

# Rule Neck (`:-`{.delightql .sigil}) {.dqlh}


```dql
young_users(*) :- valid_users(*), age < 30
```

The rule neck (`:-`{.delightql .sigil}) creates a definition that is equivalent to SQL's `CREATE TEMP VIEW`.
Delightql may choose to create temporary views to implement this abstraction, but may also
choose to do expression rewriting.
