# Effects and Directives {.dqlh}

A **directive** is a functor whose name ends in an `!` exclamation point. It an instruction to the
system to change something: rows in user tables, database objects, the session
itself, or any mutative script, i.e. it is called for its effect. A directive is still a predicate.
It is a relation whose evaluation returns a table called a **receipt**.

success | operation | ... | ...
--------|-----------|------|----------
1    | mount! | ...   | ...

Examples of calling directives:

```delightql
consult!("main-user-effect-rules.dql", "lib::user_effect_main")(*)

foo!(*)

insert!(employee(*))(*)
```

Directives may be built-in or user authored.
In either case, the following are all true:

  - all directives always end with a `!`
  - all directives always return a single row or no row at all


