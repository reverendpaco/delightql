# *`Case`* Search Function {.dqlh}

Case search evaluates conditions rather than matching values. Two syntaxes exist.


**Condition-first notation** uses -> pointing to the return value:

```delightql
students(*)
    |> %(  _:( grade > 90              ->  "A";
            grade > 80, grade <=90  ->  "B";
            grade > 70, grade <=80  ->  "C";
            grade > 60, grade <=70  ->  "D";
            _                       ->  "F") as score
            ~> count:(*) )
    |>  #(score)
```


Conditions can be conjoined with `,`{.delightql .sigil} (and). For disjunction, use the keyword **or**:

```{.delightql .numberLines }
students(*)
    |> %(  _:( grade > 90  or apple_given="true" -> "A";
              grade > 80, grade <=90            -> "B";
              grade > 70, grade <=80            -> "C";
              grade > 60, grade <=70            -> "D";
              _                                 -> "F") as score
            ~> count:(*) )
    |>  #(score)
```

Like SQL's `CASE`, the first matching clause wins.


:::::{.widen}
```delightql
members(*)
    |> (  profile_nm,
          account_nm,
          location,
          _:(
             "north india m" | location in ("in";"rajkot"), profile_nm="sally";
             "north india f" | location in ("in";"rajkot");
             "pakistan f"    | location in ("pk"; "france"), profile_nm="sally";
             "pakistan m"    | location in ("in";"rajkot")
          ) as continent )
    |>  #(profile_nm,account_nm)
```
::::::
