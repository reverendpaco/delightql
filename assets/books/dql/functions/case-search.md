# *`Case`* Search Function {.dqlh}

Case search evaluates conditions rather than matching values. Two syntaxes exist.


**Condition-first notation** uses -> pointing to the return value:

```delightql
invoice(*)
    |> %(  _:( total > 15              ->  "A";
            total > 10, total <= 15  ->  "B";
            total > 5,  total <= 10  ->  "C";
            total > 2,  total <= 5   ->  "D";
            _                        ->  "F") as tier
            ~> count:(*) )
    |>  #(tier)
```


Conditions can be conjoined with `,`{.delightql .sigil} (and). For disjunction, use the keyword **or**:

```{.delightql .numberLines }
invoice(*)
    |> %(  _:( total > 15  or billing_country = "USA" -> "A";
              total > 10, total <= 15                -> "B";
              total > 5,  total <= 10                -> "C";
              total > 2,  total <= 5                 -> "D";
              _                                      -> "F") as tier
            ~> count:(*) )
    |>  #(tier)
```

Like SQL's `CASE`, the first matching clause wins.


:::::{.widen}
```delightql
customer(*)
    |> (  first_name,
          last_name,
          country,
          _:(
             country in ("USA";"Canada"), state in ("CA";"BC") -> "north america west";
             country in ("USA";"Canada")                        -> "north america";
             country in ("Brazil";"Argentina";"Chile")          -> "south america";
             country in ("Spain";"Portugal")                    -> "iberia"
          ) as region )
    |>  #(first_name,last_name)
```
::::::
