# Effect Rules {.dqlh}

An effect rule is a rule where the functor name has a `!` in the head. It can be either lower or higher-order.


Effect rules allow the programmer to genereate their own reusable effects.


```delightql
quarantine!(Bad(*))(*) :-
    Bad(*) |> insert!(warehouse.orders_quarantine(*))(*)

stage!(*) :-
    recent_orders(*) |> temp_table!(staged)

load!(*) :-
    staged(*), +customers(customer_id), amount > 0
      |> insert!(warehouse.orders(*))(*)

```

An effect rule must

  1. Have an `!` exclamation point as the last character in the rule head name
  2. End with a directive call (either built-in or a user authored effect rule)

This second requirement guarantees that every directive maintain the contractual semantic of returning a receipt.
