
# Update {.dqlh}

To update a table, pipe a matching schema into `update!` pseudo-predicate.  The
name of the table to be updated must be the higher-order parameter -- this is
the mutation target. [Note that this implies that the DML pseudo-predicate
accepts two higher-order parameters: the contents of the query before the pipe
and the table targeted. The trailing `(*)` is RECEIPT ACCESS -- ordinary
relation access over the directive's return table -- never an input
group.]{.sidenote} The mutation target must be the source of
the data as well.

Every DML directive returns a **receipt** relation of cardinality zero or
one: one row (`success` first, then `operation`, then parameter echoes)
answers YES -- at least one row was affected; the empty relation answers
NO. There is no `success = false` row, and failure aborts rather than
encoding itself as data. DML receipts declare no `returned` payload, so
unwrapping one (`!>` or `.returned(*)`) is a category error -- the receipt
continues through `|>`. The full receipt algebra is normative in
`SEMANTICS/effect-algebra-law.md` §3.

```delightql
hr.employee!!(*)
  , Department = "Executive"
  |> $$("-------" as ssn)
  |> update!(hr.employee(*))(*)
```

```sql
UPDATE hr.employee
SET ssn = '-------'
WHERE Department = 'Executive';
```

