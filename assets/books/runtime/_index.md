
# The Delightql Runtime {.dqlh}

The delightql runtime is a set of stateful
data and contracts that anyone running a query
or script may depend upon.

The delightql runtime guarantees

 - the existence of certain namespaces
 - special namespaces for CLI, REPL and library usages
 - special rules for namespace removal and addition
 - special namespaces deemed auto-enlisted
 - the existence of certain entities: rules, functions, sigma-predicates, etc.
 - the existence of certain directives

The runtime namespacing mechanism is introspectable as tables by delightql
itself. It also doubles as a code module system and data import mechanism.

This blend of introspection and user-driven namespacing has a precedent
in **images**, a style of always-on programming promulgated by Smalltalk.

