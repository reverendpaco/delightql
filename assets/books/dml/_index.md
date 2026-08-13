# Data Manipulation Language (DML) {.dqlh}

Delightql supports SQL's tree mutation operations through three destructively sigilized pipe targets:

- `update!(T(*))(*)` --  modify existing rows
- `insert!(T(*))(*)` --  add new rows
- `delete!(T(*))(*)` --  remove rows

The `T(*)` is the **mutation target** -- a functor expression identifying
which relation to mutate.

### The `!!` Marker {.dqlh}

For `update!` and `delete!`, the source relation is also the
mutation target -- the rows being sourced are the rows being mutated.
Mark the source with `!!` to make this explicit:

```delightql
hr.employee!!(*)                   // !! = "these rows will be mutated"
  , Department = "Executive"
  |> delete!(hr.employee(*))(*)
```

The `!!` marker is required when the source is the mutation target.  The
compiler verifies that the `!!`-marked relation matches the terminal target.

For `insert!`, the source rows are **read-only input** -- even when the
source table happens to be the same as the target.  Do not use `!!` on
insert sources:

```delightql
employees(*)                       // no !! -- these rows are read-only
  , department = "Engineering"
  |> (id + 10 as id, name, department, age, salary)
  |> insert!(employees(*))(*)
```

| Terminal | Source has `!!`? | Reason |
|----------|-----------------|--------|
| `update!` | Yes | Source rows are modified in place |
| `delete!` | Yes | Source rows are removed |
| `insert!` | No | Source rows are read-only input |


## Update {.dqlh}

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


## Delete {.dqlh}

To delete from a table,  use predication to select
the rows that should be removed. The mutation target
must also be the source table and the schemas must match.


```delightql
hr.employee!!(*)
  , Department = "Executive"
  |> delete!(hr.employee(*))(*)
```

```sql
DELETE FROM hr.employee
WHERE Department = 'Executive';
```

Without filters, all rows are deleted:

```delightql
hr.employee!!(*) |> delete!(hr.employee(*))(*)
```

```sql
DELETE FROM hr.employee;
```

To keep only some rows, invert the predicate and delete the
complement:

```delightql
hr.employee!!(*)
  , Department != "Engineering"
  |> delete!(hr.employee(*))(*)
```

## Insert {.dqlh}

Use the `insert!` pseudo-predicate to insert rows.
The relation entering the `insert!` pipe must contain
a subset of the schema of the mutation target.  Any extra or erroneously
named columns are an error.

```delightql
_(LastName, FirstName, age @ "eklund", "daniel", 20)
  |> insert!(employee(*))(*)
```

```sql
INSERT INTO hr.employee (LastName, FirstName, age)
VALUES ('eklund', 'daniel', 20);
```

You may union tables and prediacte their tuples to provide input tuples:

```delightql
hr.employee(*)
  |;| new_hires(*)
  |;| transfers(, effective_date = today:())
  |> insert!(employee(*))(*)
```


```delightql
candidates(*),
  score > 90 |> (name, Department, start_date)
  |> insert!(hr.employee(*))(*)
```

```sql
INSERT INTO hr.employee (name, Department, start_date)
SELECT name, Department, start_date
FROM candidates
WHERE score > 90;
```
