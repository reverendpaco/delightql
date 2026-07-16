# *`Case`* Simple {.dqlh}

SQL's `CASE` expression serves as a switch statement. Delightql represents it
as what it is: a function, and therefore a relation.

```delightql
employee(*)
    |> +(  _:(Department @
            "engineering"  -> "tech";
            "data science" -> "tech";
            _              -> "other") as kind )
```

```sql
select
  *,
  case Department
    when 'engineering' then 'tech'
    when 'data science' then 'tech'
    else 'other'
  end as kind
from employee;
```

The ANON-FUNC sigil `_:(  )`{.delightql .sigil} creates an anonymous case
function. The F-AND sigil `->`{.delightql .sigil} separates input patterns
(left) from output values (right). The SEMI-OR sigil `;`{.delightql .sigil}
separates cases. The header Department `@`{.delightql .sigil} binds the input
to the Department column.

This is *stacked notation* applied to functions: the `->`{.delightql .sigil} acts as a special comma
that declares a functional dependency -- columns left of the arrow are inputs,
columns right are outputs.

**Named case functions**. The same notation defines reusable functions in assertion mode:

```{.delightql .numberLines .am}
department_kind(
  Department     -> kind
  ------------------
  "engineering"  -> "tech";
  "data science" -> "tech";
  _              -> "other"
)

?- employee(*)
  |> +(  department_kind:(Department) as kind )
```

The predicate `department_kind` is both a table (two columns, three rows) and a
function (input determines output). The `->`{.delightql .sigil} tells the compiler which column is
the input when invoked as a function.

Without `->`{.delightql .sigil}, the predicate is valid but not callable as a function:

```{.delightql .numberLines .am }
//WILL NOT WORK!! (at least if you want to use it for function calls)
//    .. it is a perfectly acceptable predicate
department_kind("engineering"  , "tech")
department_kind("data science" , "tech")
department_kind( _             , "other")
```

[Prolog calls these input/output declarations modes or adornments. Delightql's
`->`{.delightql .sigil} serves the same purpose: columns left of the arrow must
be instantiated inputs.]{.sidenote}
