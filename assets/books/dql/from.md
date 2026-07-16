# From {.dqlh}



Every delightql query begins with a source: one or more tables from which to
draw data. SQL calls this *selection* [In contrast to Codd's original paper,
where selection (σ) denoted row filtering--what SQL now calls
WHERE.]{.sidenote}, but we will call this *access* or *sourcing*.

## All Columns {.dqlh}

~~~{.delightql  }
employee(*)
~~~

[*]{.sidesigil}
The glob `*`{.delightql .sigil} in argument position requests all columns from a table -- equivalent to
`SELECT * FROM employee`{.sql}. This is the most common way to source data in
delightql.

## Argumentative Access {.dqlh}

Arguments within the functor bring columns into scope by position.

```{.delightql .widen}
employee(EmployeeId  , LastName ,
         FirstName   , Title    , ReportsTo,
         BirthDate   , HireDate , Address, City,
         State       , Country  , PostalCode,
         Phone       , Fax      , Email )
```

This binds each identifier to the column at that ordinal position in the table
definition. The arity must match exactly -- if `employee` has 15 columns, the
functor must have 15 arguments.

The identifiers you choose become the column names in scope of the position having
the identifier.

```delightql
employee(a, b,  c,  d,
        e,   f, g,   h, i,
        j,  k,   l, m,   n,  o )
```

This makes argumentative access error-prone for wide tables. Prefer named access
(covered later) when arity exceeds a handful of columns.


### Case insensitivity {.dqlh}

Delightql is case-insensitive. [In contrast to Prolog, where capitalization
distinguishes variables from atoms.]{.sidenote} The following all refer to the
same identifier:

 - `employeeid`
 - `EmployeeId`
 - `EMPLOYEEID`


### Stropping {.dqlh}

When a name collides with a keyword or contains illegal characters (spaces, for instance), delimit it with backticks: `` `Employee Id` ``.


## Aliasing a Table {.dqlh}

```delightql
employee(*) as e
```

The `as` keyword assigns an alias to a table. Once aliased, columns must be
accessed through the alias -- the original table name leaves scope.

```sql
select * from employee as e;
```

Aliases are often a convenience,
but become necessary in contexts like self-joins.

## Table Namespacing {.dqlh}

```delightql
hr.employee(*) as e
```

A dot-prefixed identifier namespaces the table. Here, `employee` lives within the
namespace `hr`. What this namespace represents -- schema in some databases, database
in others -- is implementation-dependent.

```sql
select * from hr.employee as e;
```

The namespace is the entire syntax _before_ the dot and may include nesting using `::`.  Namespaces are nested
like file-system folders.

```delightql
client1::production::hr.employee(*) as e
```

In the above example, `client1::production::hr` is the namespace where `client1` contains `production` which contains `hr`.

Namespaces are elements of the delightql runtime. The delightql programmer chooses the hierarchy and maps these to
source structures. For more information, see the namespacing section of DDL.

## Anonymous Table {.dqlh}


```delightql
_(1,2,3)
```
The underscore functor `_( )`{.delightql .sigil} declares a table inline, with literal values.
Commas separate columns. This is equivalent to:

```sql
select 1,2,3;

select * from (values (1,2,3));
```

The underscore is the `FULL` sigil -- a name for "no name." [The name `FULL` plays
against `NULL`: where `NULL` matches nothing and has null potency, `FULL` matches
everything and is full of potential.]{.sidenote}

Note the two anonymous tables in play: the inline table declared with `_( )`{.delightql .sigil} and
the result table returned by the query itself. The term anonymous table in this
reference denotes the former -- tables whose values are defined inline and whose
names are discarded. [An anonymous table is a query-mode construct, despite its
syntactic resemblance to assertion-mode fact instantiation.]{.sidenote}

## Stacked Notation and Multiple Rows {.dqlh}

Multiple rows are expressed with the SEMI-OR sigil `;`{.delightql .sigil}, a
disjunction operator:


```delightql
_(1,2;10,20)
```

```Sql
select 1,2
  UNION ALL
select 10,20;
```


Semicolon binds looser than comma, so rows stack naturally without parentheses.
This *stacked notation* appears throughout delightql -- anywhere multiple clauses or
rows would otherwise require repeating functor syntax. [Stacked notation is
essentially syntactic sugar: it reduces redundant functor notation in both
assertion mode and anonymous tables. See the glossary for examples.]{.sidenote}

## Naming the Columns of Anonymous Tables {.dqlh}


The columns in the examples so far have been positional -- they have ordinal
positions but no names. Delightql allows positional-only access, but also
provides syntax for naming columns.


```delightql
  _( first, second @ 1,2;10,20;100,200)
```


```Sql
select 1 as first ,2 as second
  UNION ALL
select 10,20
  UNION ALL
select 100,200;
```


The **ATOP** sigil `@`{.delightql .sigil} separates column names
(comma-delimited) from the stacked data
that follows. This three-row, two-column table now has columns named `first` and
`second`.

**ATOP** has an alternate form: three or more dashes. The following are all equivalent:


  - `@`{.delightql .sigil}
  - `---`{.delightql .sigil}
  - `--------`{.delightql .sigil}
  - `----------------------`{.delightql .sigil}

The dashed form enables formatted table literals:

```delightql
_(
 id,first_name   , last_name , email
 ----------------------------------------------------
 0,"Gusti"       , "Parlor"  , "gparlor0@phoca.cz"  ;
 1,"Diane-marie" , "McHenry" , "dmchenry1@dot.gov"  ;
 2,"Ced"         , "Mainds"  , "cmainds2@goo.ne.jp" ;
 3,"Bren"        , "Berndsen", "bberndsen3@gr.com"
)
```



## Sparse Anonymous Tables {.dqlh}

When most columns in a wide anonymous table are NULL, data rows become verbose
and error-prone. **Sparse columns** solve this: mark optional columns with `?`
in the header, then fill only the ones you need per row.

```delightql
_(column, type, nullable?, default?, check?, primary_key?, unique?
  ---------------------------------------------------------------
  "id",     "INT",    _(primary_key @ "true") ;
  "name",   "TEXT" ;
  "email",  "TEXT",   _(unique @ "true") ;
  "salary", "DECIMAL",_(check @ "salary>0"))
```

Columns without `?` are **positional** -- every row must supply them, in order.
Columns with `?` are **sparse** -- unfilled sparse columns default to NULL.
If sparse columns are used, they must come after the required columns.

### Sparse fills {.dqlh}

A sparse fill uses anonymous table syntax `_(col @ val)` to assign a value to
a named sparse column. Fills appear after the positional values in a data row:

```delightql
// Single fill
"id", "INT", _(primary_key @ "true")

// Multiple separate fills
"id", "INT", _(primary_key @ "true"), _(nullable @ "false")

// Combined fill: multiple sparse columns in one expression
"id", "INT", _(primary_key, nullable @ "true", "false")
```

In a combined fill, column names and values are matched positionally:
`primary_key` gets `"true"`, `nullable` gets `"false"`.

### No fills {.dqlh}

When a row supplies no fills, all sparse columns are NULL:

```delightql
_(a, b?, c?
  --------
  1 ;
  2 ;
  3)
```

This is equivalent to `_(a, b, c @ 1, null, null; 2, null, null; 3, null, null)`.

### All sparse {.dqlh}

A table may have no positional columns at all:

```delightql
_(x?, y?
  ------
  _(x @ 1) ;
  _(y @ 2) ;
  _(x, y @ 3, 4))
```


## The Unit Relations {.dqlh}

The anonymous-table constructor denotes exactly what is written. Zero
cells means zero columns and zero rows; a row written as `_` is a row
with no cells. Two degenerate forms follow, and they are the 0 and 1
of join:

**`_()`** — the **empty relation**: no columns, no rows. Joined with
anything, it empties the join; it is the relational FALSE. Relational
theory (Date & Darwen) names it **DUM**.

**`_(_)`** — **one row, no columns**: a fact with no attributes, bare
existence. Joined with any relation it returns that relation
unchanged — the identity of join, the relational TRUE. The theory's
name is **DEE**. (Every FROM-less `SELECT 1` in SQL has always been a
scan of this relation; in Prolog the pair is `true` and `fail`.)

**The `_`-row rule.** In row position, `_` is the empty row — not an
empty cell, not a NULL. NULL is spelled `null`. `_` as a cell among
others (`_(_, 2)`) refuses; wildcards live in case arms, not in
constructors.

`_(1)` is an ordinary table — one row, one column, value 1 — with no
special role.

Rulings and open questions:

- **`_()` in a union refuses.** The identity of UNION CORRESPONDING
  is the empty relation *of the matching schema*, which `_()`
  deliberately is not; the typed empty relation's spelling is the
  separator-with-no-rows form (`_(a, b @)`, reserved).
- **Multiplicities** (open): under proofs-preserved union,
  `_(_ ; _)` — or a zero-column projection of a ten-row table —
  holds several empty rows: true, proved twice. The bag algebra
  permits it; rule it deliberately before relying on it.
- **Zero-column projection** (`R |> ()`) would collapse a relation to
  its proofs; a door noted, not opened.

The current implementation does not honor this section: `_()` today
compiles to one row and one column containing the empty string, and
`_(_)` does not parse — see `bugs/anon-unit-relations/`.


## Higher-Order Predicates {.dqlh}



Delightql supports higher-order predicates--predicates that accept tables (or
    scalars) as parameters and return a table. SQL calls these *table-valued
functions*.

```delightql
clean_employees(hr.employee(*))(*)
```

Here, `clean_employees` is a higher-order predicate that takes `hr.employee(*)` as its
parameter.

The transpiled SQL depends on how the predicate was defined. [Defining
higher-order rules is covered in the DDL section.]{.sidenote} Given this
definition:

```{.delightql .am}
clean_employees(T(*))( * ) :-
  T(*) as t
    |> $(trim:())( t.LastName, t.FirstName)
    |> $(to_iso:())( t.BirthDate, t.HireDate)
    |> -( SSN)
```

the query `clean_employees(hr.employee(*))(*)` produces:

```sql
select
    EmployeeId,
    trim(LastName)    as LastName,
    trim(FirstName)   as FirstName,
    Title,
    ReportsTo,
    to_iso(BirthDate) as BirthDate,
    to_iso(HireDate)  as HireDate,
    Address,
    City,
    State,
    Country,
    PostalCode,
    Phone,
    Fax,
    Email
from employee;
```

Higher-order predicates are structurally typed by the columns they reference.
[This resembles duck typing: delightql has no formal type layer, but detects
which columns the predicate body requires. Any table providing those columns is
a valid argument.]{.sidenote} In this example, any table with `LastName`,
`FirstName`, `BirthDate`, `HireDate`, and `SSN` qualifies:

```delightql
clean_employees(batch.employee_2019(*))(*)
```

The pipeline form (covered later) is equivalent:


```delightql
batch.employee_2019(*)
    |> clean_employees(*)
```
