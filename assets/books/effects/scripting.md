# ETL and scripting {.dqlh}

Scripting is the act of utilizing effect rules --
builtin or authored -- to affect some change to a system.

Delightql provides the following semantic guarantees that
makes scripting principled:

 - all directive invocations return a receipt -- zero or one rules
 - all authored effect rules must end in another directive
 - the COMMA `,` short-circuits effects after any other effect that returns a zero row receipt
 - certain builtin directives -- run! chief among them -- have a well-known protocol


```delightql

recent_orders(*) :- source.orders(*), order_date >= "2026-07-01"

quarantine!(Bad(*))(*) :-
    Bad(*) |> insert!(warehouse.orders_quarantine(*))(*)

stage!(*) :-
    recent_orders(*) |> temp_table!(staged(*))(*)

load!(*) :-
    staged(*), +customers(customer_id), amount > 0
      |> insert!(warehouse.orders(*))(*)

main!(*) :-
    stage!(*) : s!
    staged(*), \+customers(customer_id) |> quarantine!(*) : q!
    load!(*) : l!

    s!(*) ; q!(*) ; l!(*)
```
