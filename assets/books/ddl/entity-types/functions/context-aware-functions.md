

# Contextual Functions {.dqlh}

The `..` sigil indicates a function that captures variables from its invocation context:
```{.delightql .am}
bitrate:(..) :-
  (bytes * 8.0)
    >> :(@ / milliseconds)
    >> round:(@, 1)
```
```delightql
track(*) |> (name, bitrate:(..) as kbps)
```

The function analyzes its body for free variables (`bytes`, `milliseconds`)
and expects them from the calling relation. This is structural typing
for functions -- any relation with those columns can use the function.

**Mixed parameters:**

Combine context capture with explicit arguments:
```{.delightql .am}
line_amount:(.., discount) :-
  (unit_price * quantity)
    >> :(@ * (1 - discount))
    >> round:(@, 2)
```
```delightql
invoice_line(*) |> (
  line_amount:(.., 0) as full_price,
  line_amount:(.., 0.25) as discounted
)
```

**Named context:**

Explicitly declare captured variables:
```{.delightql .am}
net_amount:(..{unit_price, quantity}, discount) :-
  (unit_price * quantity)
    >> :(@ * (1 - discount))
    >> round:(@, 2)
```

This makes dependencies visible in the signature and allows overriding context with explicit values:
```delightql
invoice_line(*) |> (
  net_amount:(.., 0) as from_context,
  net_amount:(1.99, quantity, 0) as explicit
)
```
