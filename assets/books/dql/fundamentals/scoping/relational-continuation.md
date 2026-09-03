
# Relational Continuation {.dqlh}

Relational continuations are defined as all strings that
 - may be appended to a valid delightql relational expression (**RELEX**) to form a new valid delightql relational expression
 - and are not themselves valid delightql RELEXs

```delightql
users(*), age<50
   |> +( :"{last_name}, {first_name}" as full_name)
```

Examples of continuations in the above valid delightql RELEX:

  - `, age<50`{.delightql}
  - `|> +( :"{last_name}, {first_name}" as full_name)`{.delightql}
  - `, age<50 |> +( :"{last_name}, {first_name}" as full_name)`{.delightql}

The continuation concept allows syntactic analysis of semantic concepts regarding scope and transformation.

## Continuation Anchor {.dqlh}

A continuation anchor is any location in a string where the substring to the left of the anchor is a RELEX and where a CONTINUATION may replace the substring to the right.  This is inclusive of a zero width empty string at the end of the string.


```delightql
users(*)               //  ①
   , age<50            //  ②
   |> ( last_name )    //  ③
```

The above example has three continuation anchors at
the end of each line.  These are the only three continuation anchors in this string.

## Current Pending Relation {.dqlh}

The current pending relation (**CPR**) is the relational value
of the relational expression (**RELEX**) to the left of a continuation anchor.
It is the ordered set of logic variables (columns) that are in scope.

```delightql
users(*) as u
   //  ①  CPR = [ u.last_name, u.first_name, u.age]
   , age<50
   //  ②  CPR = [ u.last_name, u.first_name, u.age]
   |> ( last_name )
   //  ③  CPR = [ last_name]
```

