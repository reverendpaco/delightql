# Compound Data Pathing {.dqlh}

Once compound data has been constructed, access its contents
via  *pathing* syntax.


**Array access**. Name the column, followed by a colon, the `[ ]`{.delightql .sigil} enclyph, and a 0-indexed position:

```delightql
employee(*)
  |> (Department , [LastName,FirstName] as name )
  |> ( Department, name:[0] as first_name)
```


**Record access**. Name the column, followed by a colon, the `{ }`{.delightql .sigil} enclyph, and a dot-prefixed key:

```delightql
employee(*)
  |> (Department ,
      { "FirstName": FirstName ,
        "LastName" : LastName} as name  )
  |> ( Department, name:{.FirstName})
```

**Nested access**. Chain steps with dots to descend into nested structures:

```dql
users(*)
  |> ( {last_name, "hardcoded" : [ 1,'x'] } as packet)
  |> ( packet:{.hardcoded.1})
// returns 'x' for as many records as there are users
```

```delightql
_(x @ [ 1 , 2 , {"hardcoded" : {"deeper": [ 2 , 3]}}]) as named_anon_table
  |> (x:[2.hardcoded.deeper.0])
```

**Uniform dot notation**. Unlike most languages, delightql does not alternate
between `.key` and `[index]` when pathing. All steps -- whether into a record or an
array -- use dot separation: `key.1.nested.0` rather than `key[1].nested[0]`. [The](The)
enclyph at the start (`{ }`{.delightql .sigil} or `[ ]`{.delightql .sigil}) establishes the top-level type; thereafter,
dots suffice.
