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


## Basics: Comments, Strings, and Semicolons {.dqlh}

Unlike SQL, delightql uses the double forward slash `//` for comments.
The double hyphen `--` is not a valid comment in delightql and will
fail to parse.

```delightql
// I am a valid comment
_(one@1)
```

Delightql only uses double quotes `"` to delimit a string in code.
The single quote is not an optional syntax for strings.  The backtick
is used for stropping identifiers.

```delightql
`employee`(*), last_name = "King"
```

The semicolon `;` is so pervasive in other programming languages
that this reference must make this the first syntactic clarification:
**semicolons in delightql are _NOT_ statement separators**.  Instead they are the special
operator UNION CORRESPONDING and are covered in the section on set operators.

```delightql
employee_2024(*) as x ;
  employee_2025(*) as y
// the above UNIONs the two tables
// with null padding for unshared
// columns
```

## All reserved words {.dqlh}

Delightql has very few keywords. They are

- `as`
- `not`
- `and`
- `or`
- `in`
- `true`
- `false`
- `null`
- `of` -- only for pivots
- `groups` -- only for window functions
- `rows` -- only for window functions
- `has` -- only for tree group destructuring
- `asc` and `ascending` -- only in order by
- `desc` and `descending` -- only in order by
