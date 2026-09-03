
# Anonymous Table {.dqlh}


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
