

# Citations and Symbols {.dqlh}

Citations and symbols are thematically related. They roughly
correspond to constant and symbols in other languages.

## Citations  {.dqlh}

A citation is a special calling syntax for any function of zero arity.

```delightql
version:() :  "v1.2.8"
users(*)
  |> ( :version as vrs)
```

This calling syntax makes the invocation look like a global variable
or a constant.  The only reason for using this calling syntax
is to emphasize the global nature of the citation. A function
with zero arguments is *usually* effectively a constant.


## Symbols  {.dqlh}

A symbol is special syntax for a string.

```delightql
users(*) |> +( ::ident as some_identifier )
```

Unlike a citation which requires a definition to use,
a symbol is self-denoting and can be called wherever
a string is used.  When a symbol is written
out it immediately treats the symbol as a string.


Symbols have one more special property: they only
allow identifiers or functor syntax after the `::`

 - `::identifier`
 - `::functor(a,b)`

Delightql uses symbols wherever strings requiring special
semantics are needed.


