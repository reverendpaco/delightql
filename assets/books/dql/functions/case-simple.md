# *`Case`* Simple {.dqlh}

SQL's `CASE` expression serves as a switch statement. Delightql represents it
as what it is: a function, and therefore a relation.

```delightql
employee(*)
    |> +(  _:(title @
            "IT Manager" -> "tech";
            "IT Staff"   -> "tech";
            _            -> "other") as kind )
```

```sql
select
  *,
  case title
    when 'IT Manager' then 'tech'
    when 'IT Staff' then 'tech'
    else 'other'
  end as kind
from employee;
```

The ANON-FUNC sigil `_:(  )`{.delightql .sigil} creates an anonymous case
function. The F-AND sigil `->`{.delightql .sigil} separates input patterns
(left) from output values (right). The SEMI-OR sigil `;`{.delightql .sigil}
separates cases. The header `title` `@`{.delightql .sigil} binds the input
to the `title` column.

This is *stacked notation* applied to functions: the `->`{.delightql .sigil} acts as a special comma
that declares a functional dependency -- columns left of the arrow are inputs,
columns right are outputs.

**Named case functions**. The same notation defines reusable functions in assertion mode:

```{.delightql .numberLines .am}
title_kind(
  title        -> kind
  ------------------
  "IT Manager" -> "tech";
  "IT Staff"   -> "tech";
  _            -> "other"
)

?- _(title @ "IT Staff"; "Sales Manager")
  |> +(  title_kind:(title) as kind )
```

The predicate `title_kind` is both a table (two columns, three rows) and a
function (input determines output). The `->`{.delightql .sigil} tells the compiler which column is
the input when invoked as a function.

Without `->`{.delightql .sigil}, the predicate is valid but not callable as a function:

```{.delightql .numberLines .am }
//WILL NOT WORK!! (at least if you want to use it for function calls)
//    .. it is a perfectly acceptable predicate
title_kind("IT Manager"    , "tech")
title_kind("IT Staff"      , "tech")
title_kind("Sales Manager" , "other")
```

[Prolog calls these input/output declarations modes or adornments. Delightql's
`->`{.delightql .sigil} serves the same purpose: columns left of the arrow must
be instantiated inputs.]{.sidenote}
