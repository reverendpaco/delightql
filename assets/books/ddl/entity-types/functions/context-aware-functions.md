

# Contextual Functions {.dqlh}

The `..` sigil indicates a function that captures variables from its invocation context:
```delightql
excess_index:(..) :-
  (1 + total - (interest_rate / 252))
    /-> greatest:(0.01)
    /-> ln:()
    /-> :(@ * 2)
    /-> sum:(<~ #(date))
    /-> exp:()
```
```delightql
prices(*) |> (excess_index:(..) as idx)
```

The function analyzes its body for free variables (`total`, `interest_rate`,
`date`) and expects them from the calling relation. This is structural typing
for functions -- any relation with those columns can use the function.

**Mixed parameters:**

Combine context capture with explicit arguments:
```delightql
scaled_index:(.., scale_factor) :-
  (1 + total - (interest_rate / 252))
    /-> greatest:(0.01)
    /-> ln:()
    /-> :(@ * scale_factor)
    /-> exp:()
```
```delightql
prices(*) |> (
  scaled_index:(.., 2) as double_scaled,
  scaled_index:(.., 0.5) as half_scaled
)
```

**Named context:**

Explicitly declare captured variables:
```delightql
scaled_index:(..{total, interest_rate}, scale_factor) :-
  (1 + total - (interest_rate / 252))
    /-> greatest:(0.01)
    /-> :(@ * scale_factor)
    /-> exp:()
```

This makes dependencies visible in the signature and allows overriding context with explicit values:
```delightql
prices(*) |> (
  scaled_index:(.., 2) as from_context,
  scaled_index:(manual_total, manual_rate, 2) as explicit
)
```
