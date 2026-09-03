# Common Higher-order Table Expressions {.dqlh}

Higher-order expressions may be used as common expressions
as well.  The syntax for defining the parameters is more
complicated.  The Higher-order Rules section of the reference
details.

```delightql
above(amt,I(*))(*) : I(*), amt<age
below(amt,I(*))(*) : I(*), amt>age
mark_as(l,I(*))(*) : I(*) ~> count:(*) as count |> +(l as label)
users(*)
  |> above(10)(*)
  |> below(30)(*)
  : ten_to_thirty
users(*)
  |> above(30)(*)
  |> below(40)(*)
  : thirty_to_forty
users(*)
  |> above(40)(*)
  : forty_up

ten_to_thirty(|>mark_as("ten_to_thirty")(*)) ;
forty_up(|>mark_as("fortyup")(*)) ;
thirty_to_forty(|>mark_as("thirty_to_forty")(*))
```
