#  Receipts {.dqlh}

A receipt is a single row table that is the result of calling a directive.
A receipt also includes the concept of not being returned, so the more accurate
statement is that a receipt is a zero or one row table that is the result
of calling a directive.

The presence of a single row is meant to be understood as a successful
judgement of a proposition -- a "YES", that the directive was successfull.
The lack of a row, a zero cardinality table, indicates "NO".

This cardinality and semantics allows receipts to be used in composed queries,
where the **NO** short-circuits the rest of the query, simulating an early return.
The presence of a **YES** row merely adds columns to the relation but never explodes
the cardinality of the query in which it participates.


## Receipt Schema

Every receipt GUARANTEES exactly two things:

  - the first column is `success`, value `1`;
  - the second column is `operation`, the name of the directive that produced the receipt, as written (`'temp_table!'`,
`'insert!'`),

Every remaining column is a **declared addition**: a typed column
recorded in the directive's contract.


success | operation | ... | ...
--------|-----------|------|----------
1    | mount! | ...   | ...


Some receipts from certain directives embed a table within a table (a cheat that allows richer data return,
but with the same zero-or-one cardinality that receipts demand).

Those receipts that embed a table within a table assigned to the name `returned` operate with the TEE-PIPE `!>`
a syntactic sugar for exploding out the table within and removing the receipts columns.

```delightql
foo(*) !> bar!(*)
```

is exactly equivalent to the interior-drill form:

```delightql
foo(*) |> bar!(*) |> .returned(*)
```


