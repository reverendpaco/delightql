# Function Facts Column Modality {.dqlh}

Function facts are DDL for defining named case statements.  They utilitize the
`->` (spoken **F-AND**) in their definitions to separate dimensions that
are input from dimensions that are output:


```{.delightql .am}
department_kind(
  department     -> kind
  ------------------
  "engineering"  -> "tech";
  "data science" -> "tech";
  _              -> "other"
)
```

In some respects, the invocation of the function fact is indistinguishable from
a regular function call:

```delightql
employee(*) |> +( department_kind:(title) as kind)
```

but when function facts are not-total, i.e. they lack a default arm `_ ->
"other"`,

```{.delightql .am}
partial_department_kind(
  department     -> kind
  ------------------
  "engineering"  -> "tech";
  "data science" -> "tech"
)
```

then they may be called as a table:


```delightql
partial_department_kind(*)
```


and their columns may be input or output or both:

```delightql
partial_department_kind( "engineering", kind)
// or
partial_department_kind( e, k)
```

and **still** be available to be called as functions:

```delightql
employee(*)
  |> +( partial_department_kind:(title) as kind)
```

Function fact forms that return tuples must have their
return columns accessed by name to project out the element
of the tuple:

```{.delightql .am}
shipping(zone, weight -> carrier, days
         ---------------------------
         1, 5 -> "ground", 3;
         1, 50 -> "freight", 7;
         2, 5 -> "air", 1;
         _ -> "unrouted", 0)
```

```delightql
_(zone, weight
  ------------
  1, 5; 1, 50; 2, 5; 3, 20)
  |> (shipping:(zone, weight).carrier as carrier,
      shipping:(zone, weight).days as days)
```

This is the only place in delightql where the column
is output-only **and** manifest.
