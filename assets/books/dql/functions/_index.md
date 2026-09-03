# Domain Functions {.dqlh}

Functions on domains are ordinary functions--what most programming languages
simply call "functions." The qualifier "on domains" distinguishes them from
functions on relations (table-valued functions, covered elsewhere). Here,
"domain" means data type: the addition function `+` in `SELECT Salary + bonus FROM
employee`{.sql} expects numeric operands.

SQL provides several syntaxes for functions: standard functor notation, infix
operators, and `CASE` expressions. This section covers how to call domain
functions in delightql. For defining functions, see *Function Definition*.

Domain functions can appear anywhere a column is valid -- they are substitutable for the value they compute:

 - During projection, as a transformation of a column
 - During selection, as a transformation prior to comparison
 - During grouping, as a transformation prior to grouping
 - During aggregation, as a reduction of multiple values

Delightql's default function syntax incorporates a colon: `foo:(x)` rather than
`foo(x)`. This functional functor notation distinguishes functions from
relations. [Delightql's syntax derives from Prolog, where `foo(x,y)` denotes a
relation. Functional functor notation marks the subset of relations that are
functions -- mappings that return exactly one value per input.]{.sidenote}

## Standard Function Invocation {.dqlh}

```{.delightql .numberLines}
employee(*),
  length:(LastName) > 5
  |> +( trim:(upper:(LastName)) as CapitalizedLastName)
```

Three functions appear in two contexts:

 - `length:()` in a predicate (sigma clause)
 - `trim:()` and `upper:()` in an embed, using containment composition

The colon signals to both compiler and programmer that the functor returns a domain value, not a relation.


## Function Pipe Composition {.dqlh}

The F-PIPE sigil `/->`{.delightql .sigil} composes functions left to right:

```delightql
employee(*), length:(LastName) > 5
  |> +( LastName /-> upper:() |-> trim:() as CapitalizedLastName)
```


The pipe begins with a domain expression (a column, literal, or function call).
Each subsequent function is in curried form -- the piped value fills the first
argument.

When the value belongs in a different argument position, use the F-PARAM sigil `@`:

```delightql
employee(*), length:(LastName) > 5
  |> +(  BirthDate /-> strftime:("%Y",@) as BirthYear )
```


The LAMBDA sigil `:( )`{.delightql .sigil} creates an inline function. The `@` marks where the piped value is placed.

```delightql
employee(*), length:(LastName) > 5
    |> +(  BirthDate /-> strftime:("%Y",@) |-> :( @ + 2) as BirthYearPlusTwo,
           BirthDate /-> strftime:("%Y",@) |-> sqrt:() as SqrtOfBirthYear)
```

Use the F-PIPE-END `/->>`{.delightql .sigil}   if you want the implicit parameter to be piped to the *last* argument.

```delightql
employee(*), length:(LastName) > 5
    |> +(  BirthDate /->> strftime:("%Y") |-> :( @ + 2) as BirthYearPlusTwo,
           BirthDate /->> strftime:("%Y") |-> sqrt:() as SqrtOfBirthYear)
```

A lambda has an alternate spelling where a programmer may name the parameter coming in:

```delightql
employee(*), length:(LastName) > 5
    |> +(  BirthDate /->> strftime:("%Y") |-> :(|bday| bday + 2) as BirthYearPlusTwo,
           BirthDate /->> strftime:("%Y") |-> sqrt:() as SqrtOfBirthYear)
```

Use the **LAMBDA-PARAM** `|  |` to capture and name the input variable as the named parameter.


## Domain Operators {.dqlh}

Delightql supports standard arithmetic operators:

| Sigil | Name | Arity |
|-------|------|-------|
| `*` | **OP-MULT** | Binary |
| `+` | **OP-PLUS** | Binary |
| `-` | **OP-MINUS** | Binary |
| `-` | **OP-NEGATIVE** | Unary |
| `/` | **OP-DIV** | Binary |
| `%` | **OP-MODULO** | Binary |

: Arithmetic domain operators

**No implicit precedence**. Delightql requires explicit parentheses when mixing
operators. There is no PEMDAS. [See "Order of Operations" for the conventional
rules delightql declines to adopt.]{.sidenote} This is a deliberate design
choice favoring clarity over convention.

```delightql
_(first,second,third @ 1.3,20,30)
    |> ( 1 + (first / third),
        -third / ((first * second) / 23.33),
        (third % 11) - 42)
```

```sql
select
  1+(first/third),
  -third / ((first*second)/23.33),
  (third % 11) - 42
from (select 1.3 as first, 20 as second, 30 as third);
--  1.04333333333333|-26.9192307692308|-34
```
