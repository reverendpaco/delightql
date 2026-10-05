# Higher-Order Moding Limitations {.dqlh}

Because there is no clear standard in SQL with regards to higher-order tables,
delightql has two paths whenever a higher-order abstraction is used or invoked:

 - it can lower the abstraction directly to the SQL target via a default transpilation rules
 - it can rewrite with a detail *intended* semantics into a composition of normal relational values

The former states that if delightql sees:

```delightql
json_tree("[1, 2]")(*)
```

it will lower to:

```sql
select * from json_tree('[1, 2]');
```

which is useful for well-known table-value functions like `json_each`.

The latter is exclusively the concern of the transpiler when the higher-order
rule is **programmer authored**.

## Programmer authored {.dqlh}
