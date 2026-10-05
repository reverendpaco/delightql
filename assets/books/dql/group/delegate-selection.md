# Group Delegates {.dqlh}

A group delegate is a principled mechanism for choosing one value among many during
group reduction. The group delegate generalizes the aggregate functions `max()`
and `min()` by allowing the user to choose their own ordering criteria.
The group delegate is a function in unfamiliar clothing.


The group delegate may be found **in reduction place**.
A column parenthesisized and then postfixed by the F-OVER sigil `<~`{.delightql .sigil}
followed by an explicit ordering criteria.

```delightql
track(*)
  |> %(genre_id ~> count:(*) as n, (name) <~ #(milliseconds desc))
```

With an empty ordering (bare `<~`) the delegate row is **arbitrary**:

```delightql
track(*)
  |> %(genre_id
        ~> count:(*) as n,
        (name as longest) <~ #(milliseconds desc),
        (name as arbitrary)<~ )
```

Some Sql vendors implement this functionality directly with dedicated syntax -- the `DISTINCT ON` syntax.


