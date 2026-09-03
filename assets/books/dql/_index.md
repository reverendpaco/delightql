# Data Query Language (DQL) {.dqlh}

The heart of both delightql and SQL is the query expression, also called a
table expression. A query expression is a unit of code that returns exactly one
table. Here, table is synonymous with predicate or relation, regardless of
whether the data is persisted via CREATE TABLE. [More typically, these results
are anonymous and ephemeral -- the output of execution within the
REPL.]{.sidenote}

The majority of delightql lives in this section; mastering it is prerequisite
to understanding DDL and DML.

A query's meaning is identical to the table it produces. This substitutability
is key to composability: through subqueries and CTEs, query expressions become
recursively inductive to any depth. Delightql encourages a particular style of
composition -- pipelining a relation through transformations, left to right,
with consistent associativity and scoping.
