# Facts {.dqlh}

Facts are body-less rules that define ground data. In Prolog terms, they
represent the extensional (axiomatic) portion of a program -- truths asserted
without derivation.

## Standard Facts {.dqlh}

The notation matches Prolog (minus the terminating period):

```{.delightql .am}
person(0, "Gusti", "Parlor", "gparlor0@phoca.cz")
person(1, "Diane-marie", "McHenry", "dmchenry1@dot.gov")
person(2, "Ced", "Mainds", "cmainds2@goo.ne.jp")
person(3, "Bren", "Berndsen", "bberndsen3@goodreads.com")
```

Standard facts sharing the same functor name must be co-located -- no other definitions may appear between them.

## Stacked Facts {.dqlh}

Define tabular data with headers:
```delightql
employee(
  EmployeeId , FirstName     , LastName
  -------------------
  0  , "Gusti"       , "Parlor" ;
  1  , "Diane-marie" , "McHenry" ;
  2  , "Ced"         , "Mainds"
)
```

The syntax mirrors anonymous tables, but anonymous tables
are query-mode constructs (inline data) while stacked facts are assertion-mode
constructs.

```delightql
// Anonymous table (query mode)
_(first_name, last_name
  --------------
  "Gusti"       , "Parlor" ;
  "Diane-marie" , "McHenry"
)

// Stacked fact (assertion mode)
names(first_name, last_name
  --------------
  "Gusti"       , "Parlor" ;
  "Diane-marie" , "McHenry"
)
```

## Default Implementation as Views {.dqlh}

Delightql implements facts as views by default, not tables:

```delightql
employee(
  EmployeeId , FirstName     , LastName
  -------------------
  0  , "Gusti"       , "Parlor" ;
  1  , "Diane-marie" , "McHenry" ;
  2  , "Ced"         , "Mainds"
)
```
```sql
CREATE TEMP VIEW employee AS
  SELECT 0 AS EmployeeId, 'Gusti' AS FirstName, 'Parlor' AS LastName
  UNION ALL SELECT 1, 'Diane-marie', 'McHenry'
  UNION ALL SELECT 2, 'Ced', 'Mainds';
```

This seems counterintuitive -- facts *are* data, so why not tables? The justification: typical delightql files contain only a handful of facts (test fixtures, configuration, lookup tables). For small datasets, the difference between a view over literal values and a table with inserted rows is negligible. Views avoid the overhead of table creation and cleanup.

## Sparse Stacked Facts {.dqlh}

Stacked facts support the same sparse column syntax as anonymous tables.
Mark optional columns with `?` in the header, then use `_(col @ val)` fills
in data rows:

```delightql
config(
  key, value, description?, deprecated?
  --------------------------------------
  "timeout",  30 ;
  "retries",  3,   _(description @ "max retry count") ;
  "legacy",   1,   _(deprecated @ "true")
)
```

This is equivalent to the fully-expanded form:

```delightql
config(
  key, value, description, deprecated
  ------------------------------------
  "timeout",  30,   null,                null ;
  "retries",  3,    "max retry count",   null ;
  "legacy",   1,    null,                "true"
)
```

Sparse columns reduce noise in metadata definitions and configuration
facts where most rows only set a few optional properties.
