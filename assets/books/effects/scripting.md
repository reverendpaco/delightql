# ETL and scripting {.dqlh}

Scripting is the act of utilizing effect rules --
built-in or authored -- to affect some change to a system.

Delightql provides the following semantic guarantees that
makes scripting principled:

 - all directive invocations return a receipt -- zero or one rules
 - all authored effect rules must end in another directive
 - the COMMA `,` short-circuits effects after any other effect that returns a zero row receipt
 - certain built-in directives -- run! chief among them -- have a well-known protocol


```{.delightql .am}

?- mount!("etl.sqlite", "etl")(*)
?- enlist!("main")(*)    // track, customer, and staged are read bare

landed(*) :-
    etl.partner_sale_2025_06_30(*)
      |;| etl.partner_sale_2025_07_31(*)
      |> %(*)

quarantine!(Bad(*))(*) :-
    Bad(*) |> insert!(etl.partner_sale_quarantine(*))(*)

stage!(*) :-
    landed(*)
      |> $$(lower:(trim:(buyer_email)) as buyer_email)
      |> temp_table!(staged(*))(*)

load!(Good(*))(*) :-
    Good(*), +track(*.(track_id)), +customer(, email = buyer_email), quantity > 0
      |> insert!(etl.partner_sale_line(*))(*)

main!(*) :-
    stage!(*) : s!
    staged(*), (\+track(*.(track_id)) or \+customer(, email = buyer_email))
      |> quarantine!(*) : q!
    staged(*) |> load!(*) : l!

    s!(*) ; q!(*) ; l!(*)
```
