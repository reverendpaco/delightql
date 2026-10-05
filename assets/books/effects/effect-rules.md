# Effect Rules {.dqlh}

An effect rule is a rule where the functor name has a `!` in the head. It can be either lower or higher-order.


Effect rules allow the programmer to genereate their own reusable effects.


```{.delightql .am}
?- mount!("etl.sqlite", "etl")(*)

quarantine!(Bad(*))(*) :-
    Bad(*) |> insert!(etl.partner_sale_quarantine(*))(*)

stage!(*) :-
    etl.partner_sale_2025_07_31(*) |> temp_table!(staged(*))(*)

load!(Good(*))(*) :-
    Good(*), +customer(, email = buyer_email), quantity > 0
      |> insert!(etl.partner_sale_line(*))(*)

```

An effect rule must

  1. Have an `!` exclamation point as the last character in the rule head name
  2. End with a directive call (either built-in or a user authored effect rule)

This second requirement guarantees that every directive maintain the contractual semantic of returning a receipt.
